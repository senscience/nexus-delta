package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.rdf.syntax.*
import ai.senscience.nexus.delta.sourcing.config.QueryConfig
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.postgres.Doobie
import ai.senscience.nexus.delta.sourcing.query.RefreshStrategy
import ai.senscience.nexus.testkit.clock.FixedClock
import ai.senscience.nexus.testkit.mu.NexusSuite
import munit.AnyFixture

import java.time.Instant

class ProjectionStoreSuite extends NexusSuite with Doobie.Fixture with Doobie.Assertions {

  override def munitFixtures: Seq[AnyFixture[?]] = List(doobie)

  private lazy val xas = doobie()

  private lazy val store = ProjectionStore(xas, QueryConfig(10, RefreshStrategy.Stop), clock)

  private val name     = "offset"
  private val project  = ProjectRef.unsafe("org", "proj")
  private val resource = iri"https://resource"

  private val metadata    = ProjectionMetadata("test", name, Some(project), Some(resource))
  private val progress    = ProjectionProgress(Offset.At(42L), Instant.EPOCH, 5, 2, 1)
  private val newProgress = progress.copy(offset = Offset.At(100L), processed = 100L)
  private val noProgress  = ProjectionProgress.NoProgress

  test("Return an empty offset when not found") {
    store.offset("not found").assertEquals(None)
  }

  test("Return no entries") {
    for {
      entries <- store.entries.compile.toList
      _        = entries.assertEmpty()
    } yield ()
  }

  test("Create an offset") {
    for {
      _       <- store.save(metadata, progress)
      _       <- store.offset(name).assertEquals(Some(progress))
      entries <- store.entries.compile.toList
      r        = entries.assertOneElem
      _        = assertEquals((r.name, r.project, r.resourceId, r.progress), (name, Some(project), Some(resource), progress))
      _        = assert(r.createdAt == r.updatedAt, "Created and updated at values are not identical after creation")
    } yield ()
  }

  test("Update an offset") {
    val newMetadata = ProjectionMetadata("test", name, None, None)
    for {
      _       <- store.offset(name).assertEquals(Some(progress))
      _       <- store.save(newMetadata, newProgress)
      _       <- store.offset(name).assertEquals(Some(newProgress))
      entries <- store.entries.compile.toList
      r        = entries.assertOneElem
      _        = assertEquals((r.name, r.project, r.resourceId, r.progress), (name, None, None, newProgress))
    } yield ()
  }

  test("Delete an offset") {
    for {
      _       <- store.offset(name).assertEquals(Some(newProgress))
      _       <- store.delete(name)
      entries <- store.entries.compile.toList
      _        = entries.assertEmpty()
      _       <- store.offset(name).assertEquals(None)
    } yield ()
  }

  test("Reset an offset") {
    val later      = Instant.EPOCH.plusSeconds(1000)
    val storeLater = ProjectionStore(xas, QueryConfig(10, RefreshStrategy.Stop), FixedClock.atInstant(later))

    for {
      _ <- store.save(metadata, progress)
      _ <- assertProgressAndInstants(metadata.name, progress, Instant.EPOCH, Instant.EPOCH)(store)
      _ <- storeLater.reset(metadata.name)
      _ <- assertProgressAndInstants(metadata.name, noProgress.copy(instant = later), later, later)(store)
    } yield ()
  }

  private def assertProgressAndInstants(
      name: String,
      progress: ProjectionProgress,
      createdAt: Instant,
      updatedAt: Instant
  )(
      store: ProjectionStore
  ) =
    for {
      entries <- store.entries.compile.toList
      r        = entries.assertOneElem
      _        = assertEquals((r.name, r.progress, r.createdAt, r.updatedAt), (name, progress, createdAt, updatedAt))
    } yield ()
}
