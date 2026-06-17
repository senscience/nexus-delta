package ai.senscience.nexus.delta.sourcing.projections

import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sourcing.config.QueryConfig
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.postgres.Doobie
import ai.senscience.nexus.delta.sourcing.projections.model.ProjectionTermination
import ai.senscience.nexus.delta.sourcing.query.RefreshStrategy
import ai.senscience.nexus.delta.sourcing.stream.ProjectionMetadata
import ai.senscience.nexus.testkit.mu.NexusSuite
import munit.AnyFixture

import java.time.Instant

class ProjectionTerminalStoreSuite extends NexusSuite with Doobie.Fixture with Doobie.Assertions {

  override def munitFixtures: Seq[AnyFixture[?]] = List(doobieTruncateAfterTest)

  private lazy val xas   = doobieTruncateAfterTest()
  private lazy val store = ProjectionTerminalStore(xas, QueryConfig(10, RefreshStrategy.Stop))

  private val project = ProjectRef.unsafe("org", "proj")
  private val viewId  = nxv + "view-1"

  private val viewProjection: ProjectionMetadata =
    ProjectionMetadata("test", "view-projection", project, viewId)

  private val systemProjection: ProjectionMetadata =
    ProjectionMetadata("test", "system-projection")

  private val instant1 = Instant.EPOCH
  private val instant2 = Instant.EPOCH.plusSeconds(10L)
  private val instant3 = Instant.EPOCH.plusSeconds(20L)

  private val boom     = new RuntimeException("Blazegraph is down")
  private val boomNext = new RuntimeException("Blazegraph still down")
  private val mapError = new IllegalStateException("Bad mapping")

  private def completed(
      metadata: ProjectionMetadata,
      firstOccurrence: Instant,
      lastOccurrence: Instant,
      occurrences: Long
  ): ProjectionTermination.Completed =
    ProjectionTermination.Completed(metadata, firstOccurrence, lastOccurrence, occurrences)

  private def failed(
      metadata: ProjectionMetadata,
      throwable: Throwable,
      firstOccurrence: Instant,
      lastOccurrence: Instant,
      occurrences: Long
  ): ProjectionTermination.Failed =
    ProjectionTermination.Failed(
      metadata,
      throwable.getClass.getName,
      throwable.getMessage,
      firstOccurrence,
      lastOccurrence,
      occurrences
    )

  test("recordCompletion inserts a new row with occurrences = 1") {
    val expected = completed(viewProjection, instant1, instant1, 1L)
    for {
      _ <- store.recordCompletion(viewProjection, instant1)
      _ <- store.stream.compile.toList.assertEquals(List(expected))
    } yield ()
  }

  test("Recording the same completion twice upserts and increments occurrences") {
    val expected = completed(viewProjection, instant1, instant2, 2L)
    for {
      _ <- store.recordCompletion(viewProjection, instant1)
      _ <- store.recordCompletion(viewProjection, instant2)
      _ <- store.stream.compile.toList.assertEquals(List(expected))
    } yield ()
  }

  test("recordFailure inserts a row with the throwable's class and message") {
    val expected = failed(viewProjection, boom, instant1, instant1, 1L)
    for {
      _ <- store.recordFailure(viewProjection, boom, instant1)
      _ <- store.stream.compile.toList.assertEquals(List(expected))
    } yield ()
  }

  test("Recording the same failure class twice increments occurrences and refreshes the message") {
    // Same error class as `boom`; latest message is kept.
    val expected = failed(viewProjection, boomNext, instant1, instant2, 2L)
    for {
      _ <- store.recordFailure(viewProjection, boom, instant1)
      _ <- store.recordFailure(viewProjection, boomNext, instant2)
      _ <- store.stream.compile.toList.assertEquals(List(expected))
    } yield ()
  }

  test("Different error classes for the same projection produce distinct rows ordered by last_occurrence DESC") {
    val firstFailure  = failed(viewProjection, boom, instant1, instant1, 1L)
    val secondFailure = failed(viewProjection, mapError, instant2, instant2, 1L)
    for {
      _ <- store.recordFailure(viewProjection, boom, instant1)
      _ <- store.recordFailure(viewProjection, mapError, instant2)
      _ <- store.stream.compile.toList.assertEquals(List(secondFailure, firstFailure))
    } yield ()
  }

  test("Completion and failure for the same projection coexist as separate rows") {
    val completion = completed(viewProjection, instant1, instant1, 1L)
    val failureRow = failed(viewProjection, boom, instant2, instant2, 1L)
    for {
      _ <- store.recordCompletion(viewProjection, instant1)
      _ <- store.recordFailure(viewProjection, boom, instant2)
      _ <- store.stream.compile.toList.assertEquals(List(failureRow, completion))
    } yield ()
  }

  test("System projection without project/resource records the completion with None fields") {
    val expected = completed(systemProjection, instant1, instant1, 1L)
    for {
      _ <- store.recordCompletion(systemProjection, instant1)
      _ <- store.stream.compile.toList.assertEquals(List(expected))
    } yield ()
  }

  test("Stream is ordered by last_occurrence descending") {
    val first  = completed(viewProjection, instant1, instant1, 1L)
    val second = failed(viewProjection, mapError, instant2, instant2, 1L)
    val third  = failed(viewProjection, boom, instant3, instant3, 1L)
    for {
      _ <- store.recordCompletion(viewProjection, instant1)
      _ <- store.recordFailure(viewProjection, boom, instant3)
      _ <- store.recordFailure(viewProjection, mapError, instant2)
      _ <- store.stream.compile.toList.assertEquals(List(third, second, first))
    } yield ()
  }

}
