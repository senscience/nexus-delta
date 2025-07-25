package ai.senscience.nexus.delta.elasticsearch.indexing

import ai.senscience.nexus.delta.elasticsearch.ElasticSearchViews
import ai.senscience.nexus.delta.elasticsearch.client.ElasticSearchAction.{Delete, Index}
import ai.senscience.nexus.delta.elasticsearch.client.{ElasticSearchAction, ElasticSearchClient, IndexLabel, Refresh}
import ai.senscience.nexus.delta.kernel.kamon.KamonMetricComponent
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sourcing.stream.Operation.Sink
import ai.senscience.nexus.delta.sourcing.stream.config.BatchConfig
import ai.senscience.nexus.delta.sourcing.stream.{Elem, ElemChunk}
import cats.effect.IO
import io.circe.Json
import shapeless.Typeable

/**
  * Sink that pushes json documents into an Elasticsearch index
  * @param client
  *   the ES client
  * @param batchConfig
  *   the batch configuration for the sink
  * @param index
  *   the index to push into
  * @param documentId
  *   a function that maps an elem to a documentId
  * @param routing
  *   a function that maps an elem to a routing value
  * @see
  *   https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-routing-field.html
  * @param refresh
  *   the value for the `refresh` Elasticsearch parameter
  */
final class ElasticSearchSink private (
    client: ElasticSearchClient,
    override val batchConfig: BatchConfig,
    index: IndexLabel,
    documentId: Elem[Json] => String,
    routing: Elem[Json] => Option[String],
    refresh: Refresh
) extends Sink {
  override type In = Json

  override def inType: Typeable[Json] = Typeable[Json]

  implicit private val kamonComponent: KamonMetricComponent =
    KamonMetricComponent(ElasticSearchViews.entityType.value)

  override def apply(elements: ElemChunk[Json]): IO[ElemChunk[Unit]] = {
    val actions = elements.foldLeft(Vector.empty[ElasticSearchAction]) {
      case (actions, successElem @ Elem.SuccessElem(_, _, _, _, _, json, _)) =>
        if (json.isEmpty()) {
          actions :+ Delete(index, documentId(successElem), routing(successElem))
        } else
          actions :+ Index(index, documentId(successElem), routing(successElem), json)
      case (actions, droppedElem: Elem.DroppedElem)                          =>
        actions :+ Delete(index, documentId(droppedElem), routing(droppedElem))
      case (actions, _: Elem.FailedElem)                                     => actions
    }

    if (actions.nonEmpty) {
      client
        .bulk(actions, refresh)
        .map(MarkElems(_, elements, documentId))
        .span("elasticSearchSink")
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
  ): ElasticSearchSink =
    new ElasticSearchSink(
      client,
      batchConfig,
      index,
      elem => elem.id.toString,
      _ => None,
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
  ): ElasticSearchSink =
    new ElasticSearchSink(
      client,
      batchConfig,
      index,
      elem => s"${elem.project}_${elem.id}",
      elem => Some(elem.project.toString),
      refresh
    )
}
