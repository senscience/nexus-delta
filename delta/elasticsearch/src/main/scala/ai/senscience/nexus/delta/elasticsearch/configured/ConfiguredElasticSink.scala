package ai.senscience.nexus.delta.elasticsearch.configured

import ai.senscience.nexus.delta.elasticsearch.client.ElasticSearchAction.{Delete, Index}
import ai.senscience.nexus.delta.elasticsearch.client.{ElasticSearchAction, ElasticSearchClient, IndexLabel, Refresh}
import ai.senscience.nexus.delta.elasticsearch.indexing.{ElemDocumentIdScheme, ElemRoutingScheme, MarkElems}
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sourcing.stream.Operation.Sink
import ai.senscience.nexus.delta.sourcing.stream.config.BatchConfig
import ai.senscience.nexus.delta.sourcing.stream.{Elem, ElemChunk}
import cats.effect.IO
import org.typelevel.otel4s.trace.Tracer
import shapeless3.typeable.Typeable

class ConfiguredElasticSink(
    client: ElasticSearchClient,
    override val batchConfig: BatchConfig,
    indexMap: Map[Iri, IndexLabel],
    refresh: Refresh
)(using Tracer[IO])
    extends Sink {
  override type In = ConfiguredIndexDocument

  override def inType: Typeable[ConfiguredIndexDocument] = Typeable[ConfiguredIndexDocument]

  private def partitionIndices(document: ConfiguredIndexDocument) =
    indexMap.partitionMap { case (tpe, index) =>
      Either.cond(
        document.types.contains(tpe),
        index,
        index
      )
    }

  override def apply(elements: ElemChunk[ConfiguredIndexDocument]): IO[ElemChunk[Unit]] = {
    val actions = elements.foldLeft(Vector.empty[ElasticSearchAction]) {
      case (acc, successElem @ Elem.SuccessElem(_, _, _, _, _, document, _)) =>
        val documentId = ElemDocumentIdScheme.ByProject(successElem)
        val routing    = ElemRoutingScheme.ByProject(successElem)
        if document.value.isEmpty() then {
          acc ++ indexMap.map { case (_, index) => Delete(index, documentId, routing) }
        } else {
          val (deleteIndices, updateIndices) = partitionIndices(document)
          acc ++ deleteIndices.map { index => Delete(index, documentId, routing) } ++
            acc ++ updateIndices.map { index => Index(index, documentId, routing, document.value) }
        }
      case (acc, droppedElem: Elem.DroppedElem)                              =>
        val documentId = ElemDocumentIdScheme.ByProject(droppedElem)
        val routing    = ElemRoutingScheme.ByProject(droppedElem)
        acc ++ indexMap.map { case (_, index) => Delete(index, documentId, routing) }
      case (acc, _: Elem.FailedElem)                                         => acc
    }

    if actions.nonEmpty then {
      client
        .bulk(actions, refresh)
        .map(MarkElems(_, elements, ElemDocumentIdScheme.ByProject))
        .surround("configuredElasticSink")
    } else {
      IO.pure(elements.map(_.void))
    }
  }

}

object ConfiguredElasticSink {

  def apply(
      client: ElasticSearchClient,
      config: ConfiguredIndexingConfig.Enabled,
      batchConfig: BatchConfig,
      refresh: Refresh
  )(using Tracer[IO]): ConfiguredElasticSink = {
    val indexMap = config.indices.foldLeft(Map.empty[Iri, IndexLabel]) { case (acc, configuredIndex) =>
      acc ++ configuredIndex.types.map(_ -> configuredIndex.prefixedIndex(config.prefix))
    }
    new ConfiguredElasticSink(client, batchConfig, indexMap, refresh)
  }
}
