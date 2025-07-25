package ai.senscience.nexus.delta.elasticsearch.deletion

import ai.senscience.nexus.delta.elasticsearch.ElasticSearchViews
import ai.senscience.nexus.delta.elasticsearch.deletion.ElasticSearchDeletionTask.{init, logger}
import ai.senscience.nexus.delta.elasticsearch.indexing.IndexingViewDef
import ai.senscience.nexus.delta.elasticsearch.indexing.IndexingViewDef.{ActiveViewDef, DeprecatedViewDef}
import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.sdk.deletion.ProjectDeletionTask
import ai.senscience.nexus.delta.sdk.deletion.model.ProjectDeletionReport
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import cats.effect.IO
import fs2.Stream

/**
  * Creates a project deletion step that deprecates all views within a project when a project is deleted so that the
  * coordinator stops the running Elasticsearch projections on the different Delta nodes
  */
final class ElasticSearchDeletionTask(
    currentViews: ProjectRef => Stream[IO, IndexingViewDef],
    deprecate: (ActiveViewDef, Subject) => IO[Unit]
) extends ProjectDeletionTask {

  override def apply(project: ProjectRef)(implicit subject: Subject): IO[ProjectDeletionReport.Stage] =
    logger.info(s"Starting deprecation of Elasticsearch views for '$project'") >>
      run(project)

  private def run(project: ProjectRef)(implicit subject: Subject) =
    currentViews(project)
      .evalScan(init) {
        case (acc, _: DeprecatedViewDef) => IO.pure(acc)
        case (acc, view: ActiveViewDef)  =>
          deprecate(view, subject).as(acc ++ s"Elasticsearch view '${view.ref}' has been deprecated.")
      }
      .compile
      .lastOrError
}

object ElasticSearchDeletionTask {

  private val logger = Logger[ElasticSearchDeletionTask]

  private val init = ProjectDeletionReport.Stage.empty("elasticsearch")

  def apply(views: ElasticSearchViews) =
    new ElasticSearchDeletionTask(
      project => views.currentIndexingViews(project).map(_.value),
      (v: ActiveViewDef, subject: Subject) =>
        views
          .internalDeprecate(v.ref.viewId, v.ref.project, v.rev)(subject)
          .handleErrorWith { r =>
            logger.error(s"Deprecating '$v' resulted in error: '$r'.")
          }
    )

}
