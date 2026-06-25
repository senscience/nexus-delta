package ai.senscience.nexus.delta.sdk.indexing

import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.stream.{ProjectionActivation, ProjectionActivations, ProjectionMetadata}
import cats.effect.IO
import fs2.Stream

/**
  * Resumes a project's projections as activation events come in: a whole-project activation resumes all of the
  * project's projections, while a single-projection activation resumes only that one.
  */
trait ProjectDefResumer {

  /**
    * A never-ending stream that resumes projections as activations are published: `resumeProject` for a whole-project
    * activation, `resumeProjection` for a single-projection (re)activation.
    */
  def run(resumeProject: ProjectRef => IO[Unit], resumeProjection: ProjectionMetadata => IO[Unit]): Stream[IO, Unit]
}

object ProjectDefResumer {

  def apply(activations: ProjectionActivations): ProjectDefResumer =
    (resumeProject: ProjectRef => IO[Unit], resumeProjection: ProjectionMetadata => IO[Unit]) =>
      activations.events.evalMap {
        case ProjectionActivation.ForProject(project)     => resumeProject(project)
        case ProjectionActivation.ForProjection(metadata) => resumeProjection(metadata)
      }
}
