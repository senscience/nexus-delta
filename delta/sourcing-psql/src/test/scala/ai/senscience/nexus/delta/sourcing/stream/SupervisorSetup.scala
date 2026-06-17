package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.kernel.RetryStrategyConfig
import ai.senscience.nexus.delta.sourcing.config.QueryConfig
import ai.senscience.nexus.delta.sourcing.otel.ProjectionMetrics
import ai.senscience.nexus.delta.sourcing.postgres.Doobie
import ai.senscience.nexus.delta.sourcing.projections.{ProjectionErrors, ProjectionTerminalStore, Projections}
import ai.senscience.nexus.delta.sourcing.query.{EntityTypeFilter, RefreshStrategy}
import ai.senscience.nexus.delta.sourcing.stream.config.ProjectionConfig.ClusterConfig
import ai.senscience.nexus.delta.sourcing.stream.config.{BatchConfig, ProjectionConfig}
import ai.senscience.nexus.testkit.clock.FixedClock
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.effect.{Clock, IO, Resource}
import munit.catseffect.IOFixture

import scala.concurrent.duration.*

final case class SupervisorSetup(
    supervisor: Supervisor,
    projections: Projections,
    projectionErrors: ProjectionErrors,
    terminalLog: ProjectionTerminalStore,
    activations: ProjectionActivations
)

object SupervisorSetup {

  private val defaultQueryConfig: QueryConfig = QueryConfig(10, RefreshStrategy.Delay(10.millis))

  def unapply(setup: SupervisorSetup): (Supervisor, Projections, ProjectionErrors) =
    (setup.supervisor, setup.projections, setup.projectionErrors)

  def resource(cluster: ClusterConfig, clock: Clock[IO]): Resource[IO, SupervisorSetup] = {
    val config: ProjectionConfig = ProjectionConfig(
      cluster,
      BatchConfig(3, 50.millis),
      RetryStrategyConfig.ConstantStrategyConfig(50.millis, 5),
      10.millis,
      14.days,
      1.second,
      14.days,
      defaultQueryConfig
    )
    resource(config, clock)
  }

  def resource(config: ProjectionConfig, clock: Clock[IO]): Resource[IO, SupervisorSetup] =
    Doobie.resourceDefault.flatMap { xas =>
      val projections      = Projections(xas, EntityTypeFilter.All, config.query, clock)
      val projectionErrors = ProjectionErrors(xas, config.query, clock)
      val terminalLog      = ProjectionTerminalStore(xas, config.query)
      for {
        activations <- Resource.eval(ProjectionActivations())
        supervisor  <- Supervisor(projections, projectionErrors, terminalLog, config, ProjectionMetrics.Disabled, clock)
        _           <- Resource.eval(WatchRestarts(supervisor, projections, activations))
      } yield SupervisorSetup(supervisor, projections, projectionErrors, terminalLog, activations)
    }

  trait Fixture { self: NexusSuite & FixedClock =>

    private def suiteLocalFixture(name: String, cluster: ClusterConfig): IOFixture[SupervisorSetup] =
      ResourceSuiteLocalFixture(name, resource(cluster, clock))

    val supervisor: IOFixture[SupervisorSetup]    =
      suiteLocalFixture("supervisor", ClusterConfig(1, 0))
    val supervisor3_1: IOFixture[SupervisorSetup] =
      suiteLocalFixture("supervisor3", ClusterConfig(3, 1))
  }

}
