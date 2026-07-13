package ai.senscience.nexus.delta.elasticsearch.indexing

import ai.senscience.nexus.delta.rdf.IriOrBNode.BNode
import ai.senscience.nexus.delta.rdf.Triple.{obj, predicate}
import ai.senscience.nexus.delta.rdf.Vocabulary
import ai.senscience.nexus.delta.rdf.jsonld.api.JsonLdApi
import ai.senscience.nexus.delta.rdf.jsonld.context.JsonLdContext.keywords
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sdk.indexing.MetadataFields
import ai.senscience.nexus.delta.sourcing.state.GraphResource
import ai.senscience.nexus.delta.sourcing.stream.Elem.SuccessElem
import ai.senscience.nexus.delta.sourcing.stream.Operation.Pipe
import ai.senscience.nexus.delta.sourcing.stream.{Elem, PipeRef}
import cats.effect.IO
import io.circe.syntax.{EncoderOps, KeyOps}
import io.circe.{Json, JsonObject}
import shapeless3.typeable.Typeable

/**
  * Pipe that transforms a [[GraphResource]] into a Json document
  * @param context
  *   a context to compute the compacted JSON-LD for of the [[GraphResource]]
  */
final class GraphResourceToDocument(context: ContextValue, includeContext: Boolean)(using
    JsonLdApi,
    RemoteContextResolution
) extends Pipe {
  override type In  = GraphResource
  override type Out = Json
  override def ref: PipeRef           = GraphResourceToDocument.ref
  override def inType: Typeable[In]   = Typeable[GraphResource]
  override def outType: Typeable[Out] = Typeable[Json]

  private val contextAsJson = context.contextObj.asJson

  // Extends the pipe context so that, on compaction, the metadata node becomes a nested `_nexus` object.
  private val compactionContext =
    context.merge(ContextValue(Json.obj(MetadataFields.umbrella := Json.obj(keywords.id := MetadataFields.iri))))

  /**
    * Given a [[GraphResource]] returns a JSON document. The resource id, its types and its data sit at the root while
    * the system metadata is nested under [[MetadataFields.umbrella]].
    */
  def graphToDocument(element: GraphResource): IO[Option[Json]] = {
    val graph = if element.metadataGraph.isEmpty then element.graph
    else {
      val metadataNode                = BNode.random
      val (typesGraph, metadataGraph) = element.metadataGraph.partition { case (_, p, _) =>
        p == predicate(Vocabulary.rdf.tpe)
      }
      element.graph
        .add(predicate(MetadataFields.iri), obj(metadataNode))
        ++ typesGraph
          .++(metadataGraph.replaceRootNode(metadataNode))
    }
    val json  =
      if element.source.isEmpty() then
        graph
          .toCompactedJsonLd(compactionContext)
          .map(ld => injectContext(ld.obj.asJson))
      else {
        val id = getSourceId(element.source).getOrElse(element.id.toString)
        graph
          .deleteAny(graph.rootNode, Vocabulary.rdf.tpe)
          .toCompactedJsonLd(compactionContext)
          .map(ld => injectContext(mergeJsonLd(element.source, ld.json)))
          .map(json => injectId(json, id))
      }
    json.map(j => Option.when(!j.isEmpty())(j))
  }

  override def apply(element: SuccessElem[GraphResource]): IO[Elem[Json]] =
    element.evalMapFilter(graphToDocument)

  private def getSourceId(source: Json): Option[String] =
    source.hcursor.get[String]("@id").toOption

  private def injectId(json: Json, sourceId: String) =
    json.deepMerge(JsonObject("@id" -> Json.fromString(sourceId)).asJson)

  private def injectContext(json: Json) =
    if includeContext then json.removeAllKeys(keywords.context).deepMerge(contextAsJson)
    else json.removeAllKeys(keywords.context)

  private def mergeJsonLd(a: Json, b: Json): Json =
    if a.isEmpty() then b
    else if b.isEmpty() then a
    else a.deepMerge(b)
}

object GraphResourceToDocument {

  val ref: PipeRef = PipeRef.unsafe("graph-resource-to-document")

}
