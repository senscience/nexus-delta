package ai.senscience.nexus.delta.plugins.elasticsearch.deletion

import ai.senscience.nexus.delta.plugins.elasticsearch.deletion.EventMetricsDeletionTask.report
import ai.senscience.nexus.delta.plugins.elasticsearch.metrics.EventMetrics
import ai.senscience.nexus.delta.sdk.deletion.ProjectDeletionTask
import ai.senscience.nexus.delta.sdk.deletion.model.ProjectDeletionReport
import ai.senscience.nexus.delta.sourcing.model.{Identity, ProjectRef}
import cats.effect.IO

/**
  * Creates a project deletion task that deletes event metrics pushed for this project
  */
final class EventMetricsDeletionTask(eventMetrics: EventMetrics) extends ProjectDeletionTask {

  override def apply(project: ProjectRef)(implicit subject: Identity.Subject): IO[ProjectDeletionReport.Stage] =
    eventMetrics.deleteByProject(project).as(report)

}

object EventMetricsDeletionTask {
  private val report = ProjectDeletionReport.Stage("event-metrics", "Event metrics have been successfully deleted.")

  def apply(eventMetrics: EventMetrics) = new EventMetricsDeletionTask(eventMetrics)
}
