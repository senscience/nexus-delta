package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.sourcing.config.PurgeConfig
import ai.senscience.nexus.delta.sourcing.stream.PurgeProjectionCoordinator.PurgeProjection
import ai.senscience.nexus.testkit.mu.NexusSuite
import ai.senscience.nexus.testkit.mu.ce.PatienceConfig
import cats.effect.IO
import cats.effect.kernel.Ref
import munit.AnyFixture

import java.time.Instant
import scala.concurrent.duration.*

class PurgeProjectionCoordinatorSuite extends NexusSuite with SupervisorSetup.Fixture {

  override def munitFixtures: Seq[AnyFixture[?]] = List(supervisor)

  implicit private val patienceConfig: PatienceConfig = PatienceConfig(5.seconds, 10.millis)

  private lazy val sv = supervisor().supervisor

  test("Schedule and have the supervisor executing the given purge projection") {
    val metadata        = ProjectionMetadata("test", "purge")
    val config          = PurgeConfig(100.millis, 5.days)
    val expectedInstant = Instant.EPOCH.minusMillis(config.ttl.toMillis)

    for {
      ref            <- Ref.of[IO, Instant](Instant.MIN)
      purgeProjection = PurgeProjection(metadata, config, ref.set)
      _              <- PurgeProjectionCoordinator(sv, clock, Set(purgeProjection))
      _              <- sv.describe(metadata.name)
                          .map(_.map(_.status))
                          .assertEquals(Some(ExecutionStatus.Running))
                          .eventually
      _              <- ref.get.assertEquals(expectedInstant).eventually
    } yield ()
  }
}
