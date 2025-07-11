package ai.senscience.nexus.delta.sourcing.query

import ai.senscience.nexus.delta.rdf.IriOrBNode
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.{nxv, schemas}
import ai.senscience.nexus.delta.sourcing.PullRequest.PullRequestState
import ai.senscience.nexus.delta.sourcing.PullRequest.PullRequestState.PullRequestActive
import ai.senscience.nexus.delta.sourcing.config.QueryConfig
import ai.senscience.nexus.delta.sourcing.model.*
import ai.senscience.nexus.delta.sourcing.model.Identity.{Anonymous, Subject, User}
import ai.senscience.nexus.delta.sourcing.model.ResourceRef.Latest
import ai.senscience.nexus.delta.sourcing.model.Tag.UserTag
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.postgres.Doobie
import ai.senscience.nexus.delta.sourcing.query.ElemStreamingSuite.Release
import ai.senscience.nexus.delta.sourcing.state.ScopedStateStore
import ai.senscience.nexus.delta.sourcing.state.State.ScopedState
import ai.senscience.nexus.delta.sourcing.stream.Elem.{DroppedElem, FailedElem, SuccessElem}
import ai.senscience.nexus.delta.sourcing.stream.RemainingElems
import ai.senscience.nexus.delta.sourcing.tombstone.StateTombstoneStore
import ai.senscience.nexus.delta.sourcing.{PullRequest, Scope, Serializer}
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.data.NonEmptyList
import cats.effect.IO
import doobie.syntax.all.*
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveConfiguredCodec
import io.circe.{Codec, DecodingFailure, Json}
import munit.AnyFixture

import java.time.Instant

class ElemStreamingSuite extends NexusSuite with Doobie.Fixture {

  override def munitFixtures: Seq[AnyFixture[?]] = List(doobie)

  private val qc = QueryConfig(2, RefreshStrategy.Stop)

  private lazy val xas            = doobie()
  private val entityTypes         = Some(NonEmptyList.of(PullRequest.entityType, Release.entityType))
  private lazy val elemStreaming  = ElemStreaming.stopping(xas, entityTypes, 2)
  private lazy val tombstoneStore = new StateTombstoneStore(xas)

  private lazy val prStore = ScopedStateStore[Iri, PullRequestState](
    PullRequest.entityType,
    PullRequestState.serializer,
    qc,
    xas
  )

  private lazy val releaseStore = ScopedStateStore[Iri, Release](
    Release.entityType,
    Release.serializer,
    qc,
    xas
  )

  private val alice     = User("Alice", Label.unsafe("Wonderland"))
  private val project1  = ProjectRef.unsafe("org", "proj1")
  private val project2  = ProjectRef.unsafe("org", "proj2")
  private val project3  = ProjectRef.unsafe("org2", "proj2")
  private val id1       = nxv + "1"
  private val id2       = nxv + "2"
  private val id3       = nxv + "3"
  private val id4       = nxv + "4"
  private val customTag = UserTag.unsafe("v0.1")
  private val rev       = 1

  private val epoch: Instant = Instant.EPOCH
  private val prState11      = PullRequestActive(id1, project1, rev, epoch, Anonymous, epoch, alice)
  private val prState12      = PullRequestActive(id2, project1, rev, epoch, Anonymous, epoch, alice)
  private val prState13      =
    PullRequestActive(id3, project1, rev, epoch, Anonymous, epoch, alice, Set(nxv + "Fix"))
  private val prState14      =
    PullRequestActive(id4, project1, rev, epoch, Anonymous, epoch, alice, Set(nxv + "Feature"))
  private val prState21      = PullRequestActive(id1, project2, rev, epoch, Anonymous, epoch, alice)
  private val prState34      = PullRequestActive(id4, project3, rev, epoch, Anonymous, epoch, alice)

  private val release11 = Release(nxv + "a", project1, rev, epoch, Anonymous, epoch, alice)
  private val release12 = Release(nxv + "b", project1, rev, epoch, Anonymous, epoch, alice)
  private val release21 = Release(nxv + "c", project2, rev, epoch, Anonymous, epoch, alice)

  private def decodeValue(entityType: EntityType, json: Json) =
    IO.fromEither {
      entityType match {
        case PullRequest.entityType => PullRequestState.serializer.codec.decodeJson(json).map(_.id)
        case Release.entityType     => Release.serializer.codec.decodeJson(json).map(_.id)
        case _                      => Left(DecodingFailure(s"No decoding is available for entity type $entityType", List.empty))
      }
    }

  test("Setting up the state log") {
    {
      for {
        _ <- prStore.save(prState11)                                           // 1
        _ <- prStore.save(prState12)                                           // 2
        _ <- releaseStore.save(release11)                                      // 3
        _ <- prStore.save(prState21)                                           // 4
        _ <- prStore.save(prState34)                                           // 5
        _ <- prStore.save(prState11, customTag)                                // 6
        _ <- prStore.save(prState13)                                           // 7
        _ <- releaseStore.save(release12)                                      // 8
        _ <- releaseStore.save(release12, customTag)                           // 9
        _ <- prStore.save(prState13, customTag)                                // 10
        _ <- tombstoneStore.save(PullRequest.entityType, prState13, customTag) // 11
        _ <- prStore.save(prState12, customTag)                                // 12
        _ <- releaseStore.save(release21)                                      // 13
        _ <- tombstoneStore.save(PullRequest.entityType, prState11, customTag) // 14
        _ <- prStore.save(prState14)                                           // 15
        _ <- tombstoneStore.save(Release.entityType, release12, customTag)     // 16
        _ <- prStore.save(prState14, customTag)                                // 17
      } yield ()
    }.transact(xas.write)
  }

  /** Returns streams that returns elems of Iri and elem of unit */
  private def stream(project: ProjectRef, start: Offset, selectFilter: SelectFilter) = (
    elemStreaming(Scope(project), start, selectFilter, decodeValue),
    elemStreaming(Scope(project), start, selectFilter)
  )

  test("Running a stream on latest states on project 1 from the beginning") {
    val (iri, void) = stream(project1, Offset.start, SelectFilter.latest)

    val expected = List(
      SuccessElem(PullRequest.entityType, id1, project1, epoch, Offset.at(1L), id1, rev),
      SuccessElem(PullRequest.entityType, id2, project1, epoch, Offset.at(2L), id2, rev),
      SuccessElem(Release.entityType, release11.id, project1, epoch, Offset.at(3L), release11.id, rev),
      SuccessElem(PullRequest.entityType, id3, project1, epoch, Offset.at(7L), id3, rev),
      SuccessElem(Release.entityType, release12.id, project1, epoch, Offset.at(8L), release12.id, rev),
      SuccessElem(PullRequest.entityType, id4, project1, epoch, Offset.at(15L), id4, rev)
    )

    iri.compile.toList.assertEquals(expected)
    void.compile.toList.assertEquals(expected.map(_.void))
  }

  test("Running a stream on latest states on project 1 from offset 3") {
    val (iri, void) = stream(project1, Offset.at(3L), SelectFilter.latest)

    val expected = List(
      SuccessElem(PullRequest.entityType, id3, project1, epoch, Offset.at(7L), id3, rev),
      SuccessElem(Release.entityType, release12.id, project1, epoch, Offset.at(8L), release12.id, rev),
      SuccessElem(PullRequest.entityType, id4, project1, epoch, Offset.at(15L), id4, rev)
    )

    iri.compile.toList.assertEquals(expected)
    void.compile.toList.assertEquals(expected.map(_.void))
  }

  test("Running a stream on latest states on project 1 from the beginning, filtering for types") {
    val allowedViewTypes = IriFilter.fromSet(Set(nxv + "Fix", nxv + "Feature"))
    val (iri, void)      = stream(project1, Offset.start, SelectFilter(allowedViewTypes, Tag.Latest))

    val expected = List(
      SuccessElem(PullRequest.entityType, id3, project1, epoch, Offset.at(7L), id3, rev),
      SuccessElem(PullRequest.entityType, id4, project1, epoch, Offset.at(15L), id4, rev)
    )

    iri.compile.toList.assertEquals(expected)
    void.compile.toList.assertEquals(expected.map(_.void))
  }

  test(s"Running a stream on states with tag '${customTag.value}' on project 1 from the beginning") {
    val (iri, void) = stream(project1, Offset.start, SelectFilter.tag(customTag))

    val expected = List(
      SuccessElem(PullRequest.entityType, id1, project1, epoch, Offset.at(6L), id1, rev),
      SuccessElem(Release.entityType, release12.id, project1, epoch, Offset.at(9L), release12.id, rev),
      DroppedElem(PullRequest.entityType, id3, project1, epoch, Offset.at(11L), -1),
      SuccessElem(PullRequest.entityType, id2, project1, epoch, Offset.at(12L), id2, rev),
      DroppedElem(PullRequest.entityType, id1, project1, epoch, Offset.at(14L), -1),
      DroppedElem(Release.entityType, release12.id, project1, epoch, Offset.at(16L), -1),
      SuccessElem(PullRequest.entityType, id4, project1, epoch, Offset.at(17L), id4, rev)
    )

    iri.compile.toList.assertEquals(expected)
    void.compile.toList.assertEquals(expected.map(_.void))
  }

  test(s"Running a stream on states with tag '${customTag.value}' on project 1 from offset 11") {
    val (iri, void) = stream(project1, Offset.at(11L), SelectFilter.tag(customTag))
    val expected    = List(
      SuccessElem(PullRequest.entityType, id2, project1, epoch, Offset.at(12L), id2, rev),
      DroppedElem(PullRequest.entityType, id1, project1, epoch, Offset.at(14L), -1),
      DroppedElem(Release.entityType, release12.id, project1, epoch, Offset.at(16L), -1),
      SuccessElem(PullRequest.entityType, id4, project1, epoch, Offset.at(17L), id4, rev)
    )

    iri.compile.toList.assertEquals(expected)
    void.compile.toList.assertEquals(expected.map(_.void))
  }

  test("Running a stream on latest states on project 1 from the beginning with an incomplete decode function") {
    def decodingFailure(entityType: EntityType)              =
      DecodingFailure(s"No decoding is available for entity type $entityType", List.empty)
    def incompleteDecode(entityType: EntityType, json: Json) =
      IO.fromEither {
        entityType match {
          case PullRequest.entityType => PullRequestState.serializer.codec.decodeJson(json).map(_.id)
          case _                      => Left(decodingFailure(entityType))
        }
      }

    val result                 = elemStreaming(Scope(project1), Offset.start, SelectFilter.latest, incompleteDecode)
    val releaseDecodingFailure = decodingFailure(Release.entityType)
    result.compile.toList.assertEquals(
      List(
        SuccessElem(PullRequest.entityType, id1, project1, epoch, Offset.at(1L), id1, rev),
        SuccessElem(PullRequest.entityType, id2, project1, epoch, Offset.at(2L), id2, rev),
        FailedElem(Release.entityType, release11.id, project1, epoch, Offset.at(3L), releaseDecodingFailure, rev),
        SuccessElem(PullRequest.entityType, id3, project1, epoch, Offset.at(7L), id3, rev),
        FailedElem(Release.entityType, release12.id, project1, epoch, Offset.at(8L), releaseDecodingFailure, rev),
        SuccessElem(PullRequest.entityType, id4, project1, epoch, Offset.at(15L), id4, rev)
      )
    )
  }

  test("Get the remaining elems for project 1 on latest from the beginning") {
    val expected = Some(RemainingElems(6L, epoch))
    elemStreaming
      .remaining(Scope(project1), SelectFilter.latest, Offset.start)
      .assertEquals(expected)
  }

  test("Get the remaining elems for project 1 on latest from offset 6") {
    val expected = Some(RemainingElems(3L, epoch))
    elemStreaming
      .remaining(Scope(project1), SelectFilter.latest, Offset.at(6L))
      .assertEquals(expected)
  }

  test(s"Get the remaining elems for project 1 on tag $customTag from the beginning") {
    val expected = Some(RemainingElems(4L, epoch))
    elemStreaming
      .remaining(Scope(project1), SelectFilter.tag(customTag), Offset.at(6L))
      .assertEquals(expected)
  }

  test(s"Get no remaining for an unknown project") {
    elemStreaming
      .remaining(Scope(ProjectRef.unsafe("xxx", "xxx")), SelectFilter.latest, Offset.at(6L))
      .assertEquals(None)
  }

  test(s"Find offset for the latest of $id1 in $project1") {
    StreamingQuery.offset(Scope(project1), entityTypes, SelectFilter.latest, id1, xas).assertEquals(Some(Offset.at(1L)))
  }

  test(s"Find offset for the tag $customTag of $id1 in $project1") {
    StreamingQuery
      .offset(Scope(project1), entityTypes, SelectFilter.tag(customTag), id1, xas)
      .assertEquals(Some(Offset.at(6L)))
  }

  test(s"Find no offset for an unkwon tag of $id1 in $project1") {
    StreamingQuery
      .offset(Scope(project1), entityTypes, SelectFilter.tag(UserTag.unsafe("xxx")), id1, xas)
      .assertEquals(None)
  }

  test(s"Find no offset for unknown resource in $project1") {
    StreamingQuery
      .offset(Scope(project1), entityTypes, SelectFilter.tag(customTag), nxv + "xxx", xas)
      .assertEquals(None)
  }
}

object ElemStreamingSuite {

  final private case class Release(
      id: Iri,
      project: ProjectRef,
      rev: Int,
      createdAt: Instant,
      createdBy: Subject,
      updatedAt: Instant,
      updatedBy: Subject
  ) extends ScopedState {
    override def deprecated: Boolean        = false
    override def schema: ResourceRef        = Latest(schemas + "release.json")
    override def types: Set[IriOrBNode.Iri] = Set(nxv + "Release")
  }

  private object Release {

    val entityType: EntityType = EntityType("release")

    val serializer: Serializer[Iri, Release] = {
      import ai.senscience.nexus.delta.sourcing.model.Identity.Database.*
      implicit val configuration: Configuration   = Configuration.default.withDiscriminator("@type")
      implicit val coder: Codec.AsObject[Release] = deriveConfiguredCodec[Release]
      Serializer()
    }
  }
}
