package ai.senscience.nexus.delta.sdk.schemas.job

import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.stream.*
import cats.effect.IO
import ch.epfl.bluebrain.nexus.delta.kernel.Logger

/**
  * Allows to run a revalidation of the different data resouces in the given project
  *   - Those projections are triggered directy by a a dedicated endpoint
  *   - It saves its progress and errors the same way as other projections
  *   - Unlike projections related to indexing, those tasks won't be resumed if Delta gets restarted
  *   - Running again the validation on aa project will overwrite the previous progress and the related errors
  */
trait SchemaValidationCoordinator {

  def run(project: ProjectRef): IO[Unit]

}

object SchemaValidationCoordinator {

  private val logger = Logger[SchemaValidationCoordinator]

  def projectionMetadata(project: ProjectRef): ProjectionMetadata =
    ProjectionMetadata("schema", s"schema-validate-resources-$project", Some(project), None)

  def apply(supervisor: Supervisor, schemaValidationStream: SchemaValidationStream): SchemaValidationCoordinator =
    new SchemaValidationCoordinator {

      private def compile(project: ProjectRef): IO[CompiledProjection] =
        IO.fromEither(
          CompiledProjection.compile(
            projectionMetadata(project),
            ExecutionStrategy.PersistentSingleNode,
            Source(schemaValidationStream(project, _)),
            new NoopSink[Unit]
          )
        )

      override def run(project: ProjectRef): IO[Unit] = {
        for {
          _        <- logger.info(s"Starting validation of resources for project '$project'")
          compiled <- compile(project)
          _        <- supervisor.destroy(compiled.metadata.name)
          _        <- supervisor.run(compiled)
        } yield ()
      }
    }
}
