package ai.senscience.nexus.delta.elasticsearch.metrics

import ai.senscience.nexus.delta.elasticsearch.indexing.ElemDocumentIdScheme.ByEvent
import ai.senscience.nexus.delta.elasticsearch.indexing.MarkElems
import ai.senscience.nexus.delta.elasticsearch.metrics.EventMetricsSink.empty
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sdk.model.metrics.EventMetric.ProjectScopedMetric
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.stream.Elem.{DroppedElem, FailedElem, SuccessElem}
import ai.senscience.nexus.delta.sourcing.stream.Operation.Sink
import ai.senscience.nexus.delta.sourcing.stream.config.BatchConfig
import ai.senscience.nexus.delta.sourcing.stream.{Elem, ElemChunk}
import cats.effect.IO
import cats.syntax.all.*
import shapeless3.typeable.Typeable

final class EventMetricsSink(eventMetrics: EventMetrics, override val batchConfig: BatchConfig) extends Sink {

  override def apply(elements: ElemChunk[ProjectScopedMetric]): IO[ElemChunk[Unit]] = {
    val result = elements.foldLeft(empty) {
      case (acc, success: SuccessElem[ProjectScopedMetric]) =>
        acc.copy(bulk = acc.bulk :+ success.value)
      case (acc, dropped: DroppedElem)                      =>
        acc.copy(deletes = acc.deletes :+ dropped.project -> dropped.id)
      case (acc, _: FailedElem)                             => acc
    }

    eventMetrics.index(result.bulk).map(MarkElems(_, elements, ByEvent)) <*
      result.deletes.traverse { case (project, id) =>
        eventMetrics.deleteByResource(project, id)
      }
  }

  /**
    * The underlying element type accepted by the Operation.
    */
  override type In = ProjectScopedMetric

  /**
    * @return
    *   the Typeable instance for the accepted element type
    */
  override def inType: Typeable[ProjectScopedMetric] = Typeable[ProjectScopedMetric]
}

object EventMetricsSink {
  private val empty = Acc(Vector.empty, Vector.empty)

  // Accumulator of operations to push to Elasticsearch
  final private case class Acc(bulk: Vector[ProjectScopedMetric], deletes: Vector[(ProjectRef, Iri)])
}
