package ai.senscience.nexus.delta.plugins.elasticsearch.indexing

import ai.senscience.nexus.delta.rdf.jsonld.api.{JsonLdApi, JsonLdOptions, TitaniumJsonLdApi}
import ai.senscience.nexus.delta.rdf.jsonld.context.JsonLdContext.keywords
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sourcing.state.GraphResource
import ai.senscience.nexus.delta.sourcing.stream.Elem.SuccessElem
import ai.senscience.nexus.delta.sourcing.stream.Operation.Pipe
import ai.senscience.nexus.delta.sourcing.stream.{Elem, PipeRef}
import cats.effect.IO
import io.circe.syntax.EncoderOps
import io.circe.{Json, JsonObject}
import shapeless.Typeable

/**
  * Pipe that transforms a [[GraphResource]] into a Json document
  * @param context
  *   a context to compute the compacted JSON-LD for of the [[GraphResource]]
  */
final class GraphResourceToDocument(context: ContextValue, includeContext: Boolean)(implicit
    cr: RemoteContextResolution,
    jsonLdOptions: JsonLdOptions
) extends Pipe {
  override type In  = GraphResource
  override type Out = Json
  override def ref: PipeRef                    = GraphResourceToDocument.ref
  override def inType: Typeable[GraphResource] = Typeable[GraphResource]
  override def outType: Typeable[Json]         = Typeable[Json]

  private val contextAsJson = context.contextObj.asJson

  implicit private val api: JsonLdApi = TitaniumJsonLdApi.lenient

  /** Given a [[GraphResource]] returns a JSON-LD created from the merged graph and metadata graph */
  def graphToDocument(element: GraphResource): IO[Option[Json]] = {
    val graph = element.graph ++ element.metadataGraph
    val json  =
      if (element.source.isEmpty())
        graph
          .toCompactedJsonLd(context)
          .map(ld => injectContext(ld.obj.asJson))
      else {
        val id = getSourceId(element.source).getOrElse(element.id.toString)
        graph
          .delete(graph.rootTypesNodes)
          .toCompactedJsonLd(context)
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
    if (includeContext)
      json.removeAllKeys(keywords.context).deepMerge(contextAsJson)
    else
      json.removeAllKeys(keywords.context)

  private def mergeJsonLd(a: Json, b: Json): Json =
    if (a.isEmpty()) b
    else if (b.isEmpty()) a
    else a deepMerge b
}

object GraphResourceToDocument {

  val ref: PipeRef = PipeRef.unsafe("graph-resource-to-document")

}
