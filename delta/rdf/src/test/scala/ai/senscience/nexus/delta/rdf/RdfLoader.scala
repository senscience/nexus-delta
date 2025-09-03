package ai.senscience.nexus.delta.rdf

import ai.senscience.nexus.delta.rdf.graph.{Graph, NQuads}
import ai.senscience.nexus.delta.rdf.implicits.*
import ai.senscience.nexus.delta.rdf.jsonld.api.{JsonLdApi, JsonLdOptions}
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import ai.senscience.nexus.delta.rdf.jsonld.{CompactedJsonLd, ExpandedJsonLd}
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.effect.IO

trait RdfLoader { self: NexusSuite =>

  def expandedFromJson(resourcePath: String, attributes: (String, Any)*)(implicit
      api: JsonLdApi,
      resolution: RemoteContextResolution,
      opts: JsonLdOptions
  ): IO[ExpandedJsonLd] =
    loader.jsonContentOf(resourcePath, attributes*).flatMap { json =>
      ExpandedJsonLd(json)
    }

  def expanded(resourcePath: String, attributes: (String, Any)*): IO[ExpandedJsonLd] =
    loader.jsonContentOf(resourcePath, attributes*).flatMap { json =>
      IO.fromEither(ExpandedJsonLd.expanded(json))
    }

  def graphFromJson(resourcePath: String, attributes: (String, Any)*)(implicit
      api: JsonLdApi,
      resolution: RemoteContextResolution,
      opts: JsonLdOptions
  ): IO[Graph] =
    expandedFromJson(resourcePath, attributes*).flatMap(_.toGraph)

  def compactedFromJson(
      rootId: IriOrBNode,
      context: ContextValue,
      resourcePath: String,
      attributes: (String, Any)*
  )(implicit api: JsonLdApi, resolution: RemoteContextResolution, opts: JsonLdOptions): IO[CompactedJsonLd] =
    loader
      .jsonContentOf(resourcePath, attributes*)
      .flatMap(CompactedJsonLd(rootId, context, _))

  def compactedUnsafe(
      rootId: IriOrBNode,
      context: ContextValue,
      resourcePath: String,
      attributes: (String, Any)*
  ): IO[CompactedJsonLd] =
    loader
      .jsonObjectContentOf(resourcePath, attributes*)
      .map(CompactedJsonLd.unsafe(rootId, context, _))

  def context(resourcePath: String, attributes: (String, Any)*): IO[ContextValue] =
    loader.jsonContentOf(resourcePath, attributes*).map(_.topContextValueOrEmpty)

  def nquads(rootNode: IriOrBNode, resourcePath: String, attributes: (String, Any)*): IO[NQuads] =
    loader.contentOf(resourcePath, attributes*).map(NQuads(_, rootNode))

}
