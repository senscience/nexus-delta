package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.kernel.utils.CollectionUtils.quote
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.projections.ProjectLastUpdateStore
import ai.senscience.nexus.delta.sourcing.projections.model.ProjectLastUpdate
import ai.senscience.nexus.delta.sourcing.stream.Operation.Sink
import ai.senscience.nexus.delta.sourcing.stream.ProjectLastUpdatesSink.logger
import ai.senscience.nexus.delta.sourcing.stream.config.BatchConfig
import cats.effect.IO
import shapeless3.typeable.Typeable

/**
  * Sink that computes new project last updates from the elem stream and push them to the database
  * @param store
  *   the store to insert/update to the database
  * @param batchConfig
  *   the batch configuration for the sink
  */
final class ProjectLastUpdatesSink(
    store: ProjectLastUpdateStore,
    override val batchConfig: BatchConfig
) extends Sink {

  override type In = Unit

  override def inType: Typeable[Unit] = Typeable[Unit]

  override def apply(elements: ElemChunk[Unit]): IO[ElemChunk[Unit]] = {
    val updates = computeUpdates(elements)
    logger.debug(s"Last updates have been computed for projects: ${quote(updates.keySet)}") >>
      store.save(updates.values.toList).as(elements)
  }

  private def computeUpdates(
      elements: ElemChunk[Unit]
  ): Map[ProjectRef, ProjectLastUpdate] =
    elements.foldLeft(Map.empty[ProjectRef, ProjectLastUpdate]) { case (acc, elem) =>
      val newValue = ProjectLastUpdate(elem.project, elem.instant, elem.offset)
      acc.updated(newValue.project, newValue)
    }
}

object ProjectLastUpdatesSink {

  private val logger = Logger[ProjectLastUpdatesSink]

  def apply(store: ProjectLastUpdateStore, batchConfig: BatchConfig): ProjectLastUpdatesSink =
    new ProjectLastUpdatesSink(store, batchConfig)

}
