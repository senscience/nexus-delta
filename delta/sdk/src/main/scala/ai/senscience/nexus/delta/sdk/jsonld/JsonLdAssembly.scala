package ai.senscience.nexus.delta.sdk.jsonld

import ai.senscience.nexus.delta.kernel.syntax.surround
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.RdfError
import ai.senscience.nexus.delta.rdf.graph.Graph
import ai.senscience.nexus.delta.rdf.jsonld.api.JsonLdApi
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContext, RemoteContextResolution}
import ai.senscience.nexus.delta.rdf.jsonld.{CompactedJsonLd, ExpandedJsonLd}
import ai.senscience.nexus.delta.sdk.jsonld.JsonLdRejection.InvalidJsonLdFormat
import ai.senscience.nexus.delta.sdk.model.jsonld.RemoteContextRef
import cats.effect.IO
import io.circe.Json
import org.typelevel.otel4s.trace.Tracer

/**
  * Result of the processing of the source from the [[JsonLdSourceProcessor]] which validates that the different
  * representations and the id is valid or generated
  * @param id
  *   the identifier of the resource
  * @param source
  *   the original payload
  * @param compacted
  *   its compacted json-ld representation
  * @param expanded
  *   its expanded json-ld representation
  * @param graph
  *   its graph representation
  * @param remoteContexts
  *   the resolved remote contexts
  */
final case class JsonLdAssembly(
    id: Iri,
    source: Json,
    compacted: CompactedJsonLd,
    expanded: ExpandedJsonLd,
    graph: Graph,
    remoteContexts: Set[RemoteContextRef]
) {

  /**
    * The collection of known types
    */
  def types: Set[Iri] = expanded.getTypes.getOrElse(Set.empty)
}

object JsonLdAssembly {

  def apply(
      iri: Iri,
      source: Json,
      expanded: ExpandedJsonLd,
      ctx: ContextValue,
      remoteContexts: Map[Iri, RemoteContext]
  )(using JsonLdApi, RemoteContextResolution, Tracer[IO]): IO[JsonLdAssembly] = {
    val compactedIO = expanded
      .toCompacted(ctx)
      .adaptError { case err: RdfError => InvalidJsonLdFormat(Some(iri), err) }
      .surround("compactJsonLd")
    val graphIO     = expanded.toGraph
      .adaptError { case err: RdfError => InvalidJsonLdFormat(Some(iri), err) }
      .surround("createGraph")
    IO.both(compactedIO, graphIO).map { case (compacted, graph) =>
      JsonLdAssembly(iri, source, compacted, expanded, graph, RemoteContextRef(remoteContexts))
    }
  }

  def empty(id: Iri): JsonLdAssembly =
    JsonLdAssembly(id, Json.obj(), CompactedJsonLd.empty, ExpandedJsonLd.empty, Graph.empty(id), Set.empty)

}
