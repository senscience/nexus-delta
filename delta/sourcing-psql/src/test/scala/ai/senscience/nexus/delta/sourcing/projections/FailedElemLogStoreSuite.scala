package ai.senscience.nexus.delta.sourcing.projections

import ai.senscience.nexus.delta.kernel.search.Pagination.FromPagination
import ai.senscience.nexus.delta.kernel.search.TimeRange.*
import ai.senscience.nexus.delta.kernel.search.{Pagination, TimeRange}
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sourcing.PurgeElemFailures
import ai.senscience.nexus.delta.sourcing.config.QueryConfig
import ai.senscience.nexus.delta.sourcing.model.{EntityType, FailedElemLog, ProjectRef}
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.postgres.Doobie
import ai.senscience.nexus.delta.sourcing.query.RefreshStrategy
import ai.senscience.nexus.delta.sourcing.stream.Elem.FailedElem
import ai.senscience.nexus.delta.sourcing.stream.{FailureReason, ProjectionMetadata}
import ai.senscience.nexus.testkit.clock.MutableClock
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.syntax.all.*
import munit.{AnyFixture, Location}

import java.time.Instant

class FailedElemLogStoreSuite extends NexusSuite with MutableClock.Fixture with Doobie.Fixture with Doobie.Assertions {

  override def munitFixtures: Seq[AnyFixture[?]] = List(doobie, mutableClockFixture)

  private lazy val xas = doobie()

  private val start                           = Instant.EPOCH
  private lazy val mutableClock: MutableClock = mutableClockFixture()

  private val queryConfig = QueryConfig(10, RefreshStrategy.Stop)
  private lazy val store  = FailedElemLogStore(xas, queryConfig, mutableClock)

  private def createMetadata(project: ProjectRef, id: Iri) =
    ProjectionMetadata("test", s"$project|$id", Some(project), Some(id))

  private val project1     = ProjectRef.unsafe("org", "proj")
  private val projection11 = nxv + "projection11"
  private val metadata11   = createMetadata(project1, projection11)
  private val projection12 = nxv + "projection12"
  private val metadata12   = createMetadata(project1, projection12)

  private val project2   = ProjectRef.unsafe("org", "proj2")
  private val metadata21 = createMetadata(project2, projection12)

  private val id    = nxv + "id"
  private val error = FailureReason(new RuntimeException("boom"))
  private val rev   = 1

  private val entityType                                          = EntityType("Test")
  private def createFailedElem(project: ProjectRef, offset: Long) =
    FailedElem(entityType, id, project, start.plusSeconds(offset), Offset.at(offset), error, rev)

  private val fail1 = createFailedElem(project1, 1L)
  private val fail2 = createFailedElem(project1, 2L)
  private val fail3 = createFailedElem(project1, 3L)
  private val fail4 = createFailedElem(project1, 4L)
  private val fail5 = createFailedElem(project2, 5L)

  private def populateFailures =
    saveFailedElem(metadata11, fail1) >>
      saveFailedElem(metadata12, fail2) >>
      saveFailedElem(metadata12, fail3) >>
      saveFailedElem(metadata12, fail4) >>
      saveFailedElem(metadata21, fail5)

  private val byElemOffset: FailedElemLog => Offset = _.failedElemData.offset

  private def saveFailedElem(metadata: ProjectionMetadata, failed: FailedElem) =
    mutableClock.set(failed.instant) >>
      store.save(metadata, List(failed))

  private def selectByName(metadata: ProjectionMetadata) = ProjectionSelector.Name(metadata.name)

  private def selectById(metadata: ProjectionMetadata) =
    (metadata.project, metadata.resourceId).mapN(ProjectionSelector.ProjectId(_, _))

  private def assertStream(metadata: ProjectionMetadata, offset: Offset, expected: List[FailedElem])(implicit
      loc: Location
  ) = {
    val expectedOffsets = expected.map(_.offset)
    for {
      _ <- store.stream(ProjectionSelector.Name(metadata.name), offset).map(byElemOffset).assert(expectedOffsets)
      _ <- selectById(metadata).traverse { selector =>
             store.stream(selector, offset).map(byElemOffset).assert(expectedOffsets)
           }
    } yield ()
  }

  private def countAndListFor(metadata: ProjectionMetadata, pagination: FromPagination, timeRange: TimeRange)(
      expectedCount: Long,
      expected: FailedElem*
  )(implicit loc: Location) = {
    val byName          = selectByName(metadata)
    val byId            = selectById(metadata)
    val expectedOffsets = expected.map(_.offset).toList
    for {
      _ <- store.count(byName, timeRange).assertEquals(expectedCount)
      _ <- store
             .list(byName, pagination, timeRange)
             .map(_.map(byElemOffset))
             .assertEquals(expectedOffsets)
      _ <- byId.traverse { selector =>
             store.count(selector, timeRange).assertEquals(expectedCount) >>
               store.list(selector, pagination, timeRange).map(_.map(byElemOffset)).assertEquals(expectedOffsets)
           }
    } yield ()
  }

  test("Insert empty list of failures") {
    for {
      _ <- store.save(metadata11, List.empty)
      _ <- assertStream(metadata11, Offset.Start, List.empty)
    } yield ()
  }

  test("Insert several failures") {
    populateFailures
  }

  test(s"Get stream of failures for ${metadata11.name}") {
    for {
      entries <- store.stream(selectByName(metadata11), Offset.start).compile.toList
      r        = entries.assertOneElem
      _        = assertEquals(r.projectionMetadata, metadata11)
      _        = assertEquals(r.ordering, Offset.At(1L))
      _        = assertEquals(r.instant, fail1.instant)
      elem     = r.failedElemData
      _        = assertEquals(elem.offset, Offset.At(1L))
      _        = assertEquals(elem.reason.`type`, "UnexpectedError")
      _        = assertEquals(elem.id, id)
      _        = assertEquals(elem.entityType, entityType)
      _        = assertEquals(elem.rev, rev)
      _        = assertEquals(elem.project, Some(project1))
    } yield ()
  }

  test(s"Get a stream of all failures") {
    assertStream(metadata12, Offset.start, List(fail2, fail3, fail4))
  }

  test("Get an empty stream for an unknown projection") {
    val unknownMetadata = createMetadata(ProjectRef.unsafe("xxx", "xxx"), nxv + "xxx")
    assertStream(unknownMetadata, Offset.start, List.empty)
  }

  test(s"List all failures") {
    countAndListFor(metadata12, Pagination.OnePage, Anytime)(3L, fail2, fail3, fail4)
  }

  test(s"Paginate failures to get one result") {
    countAndListFor(metadata12, FromPagination(1, 1), Anytime)(3L, fail3)
  }

  test(s"Paginate failures to get the last results ") {
    countAndListFor(metadata12, FromPagination(1, 2), Anytime)(3L, fail3, fail4)
  }

  test(s"Count and list failures after a given time") {
    val after = After(fail3.instant)
    countAndListFor(metadata12, Pagination.OnePage, after)(2L, fail3, fail4)
  }

  test(s"Count and list  failures before a given time") {
    val before = Before(fail3.instant)
    countAndListFor(metadata12, Pagination.OnePage, before)(2L, fail2, fail3)
  }

  test(s"Count and list  failures within the time window") {
    val between = Between.unsafe(fail2.instant.plusMillis(1L), fail3.instant.plusMillis(1L))
    countAndListFor(metadata12, Pagination.OnePage, between)(1L, fail3)
  }

  test("Fetch latest failures") {
    val expected = List(fail5.offset, fail4.offset, fail3.offset, fail2.offset)
    store.latest(4).map(_.map(_.failedElemData.offset)).assertEquals(expected)
  }

  private def countSinceStart = store.count(After(start))

  test("Purge failures before given instant") {
    val purgeElemFailures = new PurgeElemFailures(xas)

    for {
      _ <- countSinceStart.assertEquals(5L)
      _ <- purgeElemFailures(start.minusMillis(500L))
      // no elements are deleted before the start instant
      _ <- countSinceStart.assertEquals(5L)
      _ <- purgeElemFailures(start.plusSeconds(10L))
      // all elements were deleted after 14 days
      _ <- countSinceStart.assertEquals(0L)
    } yield ()
  }

  test("Delete fixtures for the given projection") {
    for {
      _ <- populateFailures
      _ <- countSinceStart.assertEquals(5L)
      _ <- store.deleteEntriesForProjection(metadata11.name)
      _ <- countSinceStart.assertEquals(4L)
      _ <- store.deleteEntriesForProjection(metadata12.name)
      _ <- countSinceStart.assertEquals(1L)
    } yield ()
  }

}
