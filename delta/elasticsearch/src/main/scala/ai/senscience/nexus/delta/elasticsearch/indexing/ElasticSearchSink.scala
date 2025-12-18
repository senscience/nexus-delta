package ai.senscience.nexus.delta.elasticsearch.indexing

import ai.senscience.nexus.delta.elasticsearch.client.ElasticSearchAction.{Delete, Index}
import ai.senscience.nexus.delta.elasticsearch.client.{ElasticSearchAction, ElasticSearchClient, IndexLabel, Refresh}
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sourcing.stream.Operation.Sink
import ai.senscience.nexus.delta.sourcing.stream.config.BatchConfig
import ai.senscience.nexus.delta.sourcing.stream.{Elem, ElemChunk}
import cats.effect.IO
import io.circe.Json
import org.typelevel.otel4s.trace.Tracer
import shapeless3.typeable.Typeable

/**
  * Sink that pushes json documents into an Elasticsearch index
  * @param client
  *   the ES client
  * @param batchConfig
  *   the batch configuration for the sink
  * @param index
  *   the index to push into
  * @param idScheme
  *   how to get the document id from the incoming elem
  * @param routingScheme
  *   how to route the document to a given shard
  * @see
  *   https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-routing-field.html
  * @param refresh
  *   the value for the `refresh` Elasticsearch parameter
  */
final class ElasticSearchSink private (
    client: ElasticSearchClient,
    override val batchConfig: BatchConfig,
    index: IndexLabel,
    idScheme: ElemDocumentIdScheme,
    routingScheme: ElemRoutingScheme,
    refresh: Refresh
)(using Tracer[IO])
    extends Sink {
  override type In = Json

  override def inType: Typeable[Json] = Typeable[Json]

  override def apply(elements: ElemChunk[Json]): IO[ElemChunk[Unit]] = {
    val actions = elements.foldLeft(Vector.empty[ElasticSearchAction]) {
      case (acc, successElem @ Elem.SuccessElem(_, _, _, _, _, json, _)) =>
        if json.isEmpty() then {
          acc :+ Delete(index, idScheme(successElem), routingScheme(successElem))
        } else acc :+ Index(index, idScheme(successElem), routingScheme(successElem), json)
      case (acc, droppedElem: Elem.DroppedElem)                          =>
        acc :+ Delete(index, idScheme(droppedElem), routingScheme(droppedElem))
      case (acc, _: Elem.FailedElem)                                     => acc
    }

    if actions.nonEmpty then {
      client
        .bulk(actions, refresh)
        .map(MarkElems(_, elements, idScheme))
        .surround("elasticSearchSink")
    } else {
      IO.pure(elements.map(_.void))
    }
  }
}

object ElasticSearchSink {

  /**
    * @param client
    *   the ES client
    * @param batchConfig
    *   the batch configuration for the sink
    * @param index
    *   the index to push into
    * @param refresh
    *   the value for the `refresh` Elasticsearch parameter
    * @return
    *   an ElasticSearchSink for states
    */
  def states(
      client: ElasticSearchClient,
      batchConfig: BatchConfig,
      index: IndexLabel,
      refresh: Refresh
  )(using Tracer[IO]): ElasticSearchSink =
    new ElasticSearchSink(
      client,
      batchConfig,
      index,
      ElemDocumentIdScheme.ById,
      ElemRoutingScheme.Never,
      refresh
    )

  /**
    * @param client
    *   the ES client
    * @param batchConfig
    *   the batch configuration for the sink
    * @param index
    *   the index to push into
    * @param refresh
    *   the value for the `refresh` Elasticsearch parameter
    */
  def mainIndexing(
      client: ElasticSearchClient,
      batchConfig: BatchConfig,
      index: IndexLabel,
      refresh: Refresh
  )(using Tracer[IO]): ElasticSearchSink =
    new ElasticSearchSink(
      client,
      batchConfig,
      index,
      ElemDocumentIdScheme.ByProject,
      ElemRoutingScheme.ByProject,
      refresh
    )
}
