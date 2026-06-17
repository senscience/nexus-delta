package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import cats.effect.IO
import cats.syntax.all.*
import fs2.Stream

/**
  * Resumes passivated projections as activation events come in: when a project becomes active again all of its active
  * views are resumed, and when a single projection is (re)activated its view is resumed. It also exposes project
  * activity so coordinators can gate the initial start of their views on it. Generic over the view definition type `V`.
  */
trait ProjectionResumer[V] {

  /** A never-ending stream that resumes projections (via `resume`) as activations are published. */
  def run(resume: V => IO[Unit]): Stream[IO, Unit]

  /** Whether the given project is currently active. */
  def isActive(project: ProjectRef): IO[Boolean]
}

object ProjectionResumer {

  def apply[V](
      module: String,
      activeViews: ProjectRef => Stream[IO, V],
      fetchView: (ProjectRef, Iri) => IO[Option[V]],
      projectActivity: ProjectActivity,
      activations: ProjectionActivations
  ): ProjectionResumer[V] = new ProjectionResumer[V] {

    override def isActive(project: ProjectRef): IO[Boolean] = projectActivity.isActive(project)

    override def run(resume: V => IO[Unit]): Stream[IO, Unit] =
      activations.events.evalMap {
        case ProjectionActivation.ForProject(project)     =>
          // The project just became active: resume all of its active views.
          activeViews(project).evalMap(resume).compile.drain
        case ProjectionActivation.ForProjection(metadata) =>
          IO.whenA(metadata.module == module) {
            (metadata.project, metadata.resourceId).traverseN { case (project, id) =>
              fetchView(project, id).flatMap(_.traverse(resume))
            }.void
          }
      }
  }
}
