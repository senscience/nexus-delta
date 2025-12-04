package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sourcing.PullRequest
import ai.senscience.nexus.delta.sourcing.PullRequest.PullRequestState.PullRequestActive
import ai.senscience.nexus.delta.sourcing.config.QueryConfig
import ai.senscience.nexus.delta.sourcing.model.Identity.{Anonymous, User}
import ai.senscience.nexus.delta.sourcing.model.{Label, ProjectRef}
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.postgres.Doobie
import ai.senscience.nexus.delta.sourcing.query.RefreshStrategy
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.syntax.all.*
import doobie.syntax.all.*
import munit.{AnyFixture, Location}

import java.time.Instant

class ProjectLastUpdateWritesSuite extends NexusSuite with Doobie.Fixture {

  override def munitFixtures: Seq[AnyFixture[?]] = List(doobie)

  private lazy val xas = doobie()
  private val qc       = QueryConfig(2, RefreshStrategy.Stop)

  private lazy val prStore = PullRequest.stateStore(xas, qc)

  private val alice    = User("Alice", Label.unsafe("Wonderland"))
  private val project1 = ProjectRef.unsafe("org", "proj1")
  private val project2 = ProjectRef.unsafe("org", "proj2")

  private val id1 = nxv + "1"
  private val id2 = nxv + "2"
  private val id3 = nxv + "3"

  private val epoch: Instant = Instant.EPOCH
  private val rev            = 1

  private val prState11 = PullRequestActive(id1, project1, rev, epoch, Anonymous, epoch, alice)
  private val prState12 = PullRequestActive(id2, project1, rev, epoch, Anonymous, epoch.plusSeconds(1L), alice)
  private val prState13 =
    PullRequestActive(id3, project1, rev, epoch, Anonymous, epoch.plusSeconds(2L), alice, Set(nxv + "Fix"))
  private val prState21 = PullRequestActive(id1, project2, rev, epoch, Anonymous, epoch.plusSeconds(3L), alice)

  private def queryLastUpdates(offset: Offset, batchSize: Int, expected: (ProjectRef, Instant, Offset)*)(using
      Location
  ) =
    ProjectLastUpdateWrites
      .query(offset, batchSize)
      .to[List]
      .transact(xas.read)
      .map {
        _.map { elem => (elem.project, elem.instant, elem.offset) }
      }
      .assertEquals(expected.toList)

  test("Setting up the state log and check for the last updates") {
    val lastUpdateProj1 = (project1, epoch.plusSeconds(2L), Offset.at(3L))
    val lastUpdateProj2 = (project2, epoch.plusSeconds(3L), Offset.at(4L))
    List(prState11, prState12, prState13, prState21).traverse(prStore.save).transact(xas.write) >>
      // Only an elem for project1 should be returned because of the batch size
      queryLastUpdates(Offset.start, 1, lastUpdateProj1) >>
      // Only an elem for project2 should be returned because of the offset
      queryLastUpdates(Offset.at(3L), 1, lastUpdateProj2) >>
      // Both entries should be returned
      queryLastUpdates(Offset.start, 2, lastUpdateProj1, lastUpdateProj2)
  }
}
