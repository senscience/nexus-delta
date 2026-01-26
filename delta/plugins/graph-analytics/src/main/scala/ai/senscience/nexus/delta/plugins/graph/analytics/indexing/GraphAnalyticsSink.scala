package ai.senscience.nexus.delta.plugins.graph.analytics.indexing

import ai.senscience.nexus.delta.elasticsearch.client.{ElasticSearchAction, ElasticSearchClient, ElasticSearchRequest, IndexLabel, Refresh}
import ai.senscience.nexus.delta.elasticsearch.indexing.{ElemDocumentIdScheme, MarkElems}
import ai.senscience.nexus.delta.plugins.graph.analytics.indexing.GraphAnalyticsResult.{Index, Noop, UpdateByQuery}
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sourcing.stream.Elem.{DroppedElem, FailedElem, SuccessElem}
import ai.senscience.nexus.delta.sourcing.stream.ElemChunk
import ai.senscience.nexus.delta.sourcing.stream.Operation.Sink
import ai.senscience.nexus.delta.sourcing.stream.config.BatchConfig
import cats.effect.IO
import io.circe.literal.*
import io.circe.syntax.EncoderOps
import org.typelevel.otel4s.trace.Tracer
import shapeless3.typeable.Typeable

/**
  * Sink that pushes the [[GraphAnalyticsResult]] to the given index
  * @param client
  *   the ES client
  * @param batchConfig
  *   the batch configuration for the sink
  * @param index
  *   the index to push into
  */
final class GraphAnalyticsSink(
    client: ElasticSearchClient,
    override val batchConfig: BatchConfig,
    index: IndexLabel
)(using Tracer[IO])
    extends Sink {

  override type In = GraphAnalyticsResult

  override def inType: Typeable[GraphAnalyticsResult] = Typeable[GraphAnalyticsResult]

  private def relationshipsQuery(updates: Map[Iri, Set[Iri]]): ElasticSearchRequest = {
    val terms  = updates.map { case (id, _) => id.asJson }.asJson
    ElasticSearchRequest(
      "query"  ->
        json"""
          {
            "bool": {
              "filter": {
                "terms": {
                  "references.@id": $terms
                }
              }
            }
          }""",
      "script" -> json"""{
                          "id": "updateRelationships",
                          "params": {
                            "updates": $updates
                          }
                        }"""
    )
  }

  override def apply(elements: ElemChunk[GraphAnalyticsResult]): IO[ElemChunk[Unit]] = {
    val result = elements.foldLeft(GraphAnalyticsSink.empty) {
      case (acc, success: SuccessElem[GraphAnalyticsResult]) =>
        success.value match {
          case Noop                     => acc
          case UpdateByQuery(id, types) => acc.update(id, types)
          case g: Index                 =>
            val bulkAction = ElasticSearchAction.Index(index, ElemDocumentIdScheme.ById(success), None, g.asJson)
            acc.add(bulkAction).update(g.id, g.types)
        }
      // TODO: handle correctly the deletion of individual resources when the feature is implemented
      case (acc, _: DroppedElem)                             => acc
      case (acc, _: FailedElem)                              => acc
    }

    client.bulk(result.bulk, Refresh.True).map(MarkElems(_, elements, ElemDocumentIdScheme.ById)) <*
      client.updateByQuery(relationshipsQuery(result.updates), Set(index.value))
  }.surround("graphAnalyticsSink")
}

object GraphAnalyticsSink {

  private val empty = Acc(List.empty, Map.empty)

  // Accumulator of operations to push to Elasticsearch
  final private case class Acc(bulk: List[ElasticSearchAction], updates: Map[Iri, Set[Iri]]) {
    def add(index: ElasticSearchAction): Acc  = copy(bulk = index :: bulk)
    def update(id: Iri, types: Set[Iri]): Acc = copy(updates = updates + (id -> types))
  }

}
