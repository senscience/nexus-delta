package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.sourcing.projections.Projections
import ai.senscience.nexus.testkit.mu.ce.{CatsEffectEventually, PatienceConfig}
import cats.effect.IO
import munit.{Assertions, CatsEffectAssertions, Location}

trait ProjectionAssertions extends CatsEffectEventually {
  self: Assertions with CatsEffectAssertions =>

  /**
    * Wait for the given project to complete its execution on the supervisor
    */
  def waitProjectionCompletion(supervisor: Supervisor, projectionName: String)(implicit
      loc: Location,
      patience: PatienceConfig
  ): IO[Unit] =
    supervisor
      .describe(projectionName)
      .map(_.map(_.status))
      .assertEquals(Some(ExecutionStatus.Completed))
      .eventually

  /**
    * Expect the projection to reach the expected progress
    */
  def assertProgress(projections: Projections, projectionName: String)(
      expected: ProjectionProgress
  )(implicit loc: Location, patience: PatienceConfig): IO[Unit] =
    projections
      .progress(projectionName)
      .assertEquals(Some(expected))
      .eventually

}
