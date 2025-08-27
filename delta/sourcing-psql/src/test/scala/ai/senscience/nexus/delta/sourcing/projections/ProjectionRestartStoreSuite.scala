package ai.senscience.nexus.delta.sourcing.projections

import ai.senscience.nexus.delta.sourcing.config.QueryConfig
import ai.senscience.nexus.delta.sourcing.model.Identity.Anonymous
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.postgres.Doobie
import ai.senscience.nexus.delta.sourcing.projections.model.ProjectionRestart
import ai.senscience.nexus.delta.sourcing.query.RefreshStrategy
import ai.senscience.nexus.testkit.mu.NexusSuite
import munit.AnyFixture

import java.time.Instant

class ProjectionRestartStoreSuite extends NexusSuite with Doobie.Fixture with Doobie.Assertions {

  override def munitFixtures: Seq[AnyFixture[?]] = List(doobie)

  private lazy val xas = doobie()

  private lazy val store = new ProjectionRestartStore(xas, QueryConfig(10, RefreshStrategy.Stop))

  private val pr1 = ProjectionRestart("proj1", Offset.start, Instant.EPOCH, Anonymous)
  private val pr2 = ProjectionRestart("proj2", Offset.start, Instant.EPOCH.plusSeconds(5L), Anonymous)

  test("Save a projection restart") {
    store.save(pr1).assert
  }

  test("Save a second projection restart") {
    store.save(pr2).assert
  }

  test("Stream projection restarts") {
    store
      .stream(Offset.start)
      .assert((Offset.at(1L), pr1), (Offset.at(2L), pr2))
  }

  test("Delete older restarts and stream again") {
    for {
      _ <- store.deleteExpired(Instant.EPOCH.plusSeconds(2L))
      _ <- store.stream(Offset.start).assert((Offset.at(2L), pr2))
    } yield ()
  }

  test("Acknowledge restart 2 and stream again") {
    for {
      _ <- store.acknowledge(Offset.at(2L))
      _ <- store.stream(Offset.start).assert()
    } yield ()
  }

}
