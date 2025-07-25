package ai.senscience.nexus.delta.sourcing

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sourcing.EvaluationError.{EvaluationTagFailure, EvaluationTimeout}
import ai.senscience.nexus.delta.sourcing.PullRequest.PullRequestCommand.*
import ai.senscience.nexus.delta.sourcing.PullRequest.PullRequestEvent.{PullRequestCreated, PullRequestMerged, PullRequestTagged}
import ai.senscience.nexus.delta.sourcing.PullRequest.PullRequestRejection.*
import ai.senscience.nexus.delta.sourcing.PullRequest.PullRequestState.{PullRequestActive, PullRequestClosed}
import ai.senscience.nexus.delta.sourcing.PullRequest.{PullRequestCommand, PullRequestEvent, PullRequestState}
import ai.senscience.nexus.delta.sourcing.ScopedEntityDefinition.Tagger
import ai.senscience.nexus.delta.sourcing.config.QueryConfig
import ai.senscience.nexus.delta.sourcing.implicits.*
import ai.senscience.nexus.delta.sourcing.model.*
import ai.senscience.nexus.delta.sourcing.model.EntityDependency.DependsOn
import ai.senscience.nexus.delta.sourcing.model.Identity.{Anonymous, User}
import ai.senscience.nexus.delta.sourcing.model.Tag.{latest, UserTag}
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.postgres.Doobie
import ai.senscience.nexus.delta.sourcing.query.RefreshStrategy
import ai.senscience.nexus.delta.sourcing.tombstone.{EventTombstoneStore, StateTombstoneStore}
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.syntax.all.*
import doobie.syntax.all.*
import io.circe.Decoder
import munit.{AnyFixture, Location}

import java.time.Instant
import scala.concurrent.duration.*

class ScopedEventLogSuite extends NexusSuite with Doobie.Fixture {

  override def munitFixtures: Seq[AnyFixture[?]] = List(doobie)

  private lazy val xas = doobie()

  private val queryConfig = QueryConfig(10, RefreshStrategy.Delay(500.millis))

  private lazy val eventTombstoneStore = new EventTombstoneStore(xas)
  private lazy val stateTombstoneStore = new StateTombstoneStore(xas)

  private lazy val eventStore = PullRequest.eventStore(queryConfig)

  private lazy val stateStore = PullRequest.stateStore(xas, queryConfig)

  private val maxDuration = 100.millis

  private val id   = nxv + "id"
  private val id2  = nxv + "id2"
  private val proj = ProjectRef.unsafe("org", "proj")

  private val opened = PullRequestCreated(id, proj, Instant.EPOCH, Anonymous)
  private val tagged = PullRequestTagged(id, proj, 2, 1, Instant.EPOCH, Anonymous)
  private val merged = PullRequestMerged(id, proj, 3, Instant.EPOCH, Anonymous)

  private val state1 = PullRequestActive(id, proj, 1, Instant.EPOCH, Anonymous, Instant.EPOCH, Anonymous)
  private val state2 = PullRequestActive(id, proj, 2, Instant.EPOCH, Anonymous, Instant.EPOCH, Anonymous)
  private val state3 = PullRequestClosed(id, proj, 3, Instant.EPOCH, Anonymous, Instant.EPOCH, Anonymous)

  private val tagActive = UserTag.unsafe("active")
  private val tagClosed = UserTag.unsafe("closed")

  implicit val user: User = User("writer", Label.unsafe("realm"))

  private def assertStateNotFound(project: ProjectRef, id: Iri)(implicit loc: Location) =
    eventLog.stateOr(project, id, NotFound).interceptEquals(NotFound)

  private lazy val eventLog: ScopedEventLog[
    Iri,
    PullRequestState,
    PullRequestCommand,
    PullRequestEvent,
    PullRequest.PullRequestRejection
  ] = ScopedEventLog(
    PullRequest.entityType,
    eventStore,
    stateStore,
    PullRequest.stateMachine,
    (id: Iri, c: PullRequestCommand) => AlreadyExists(id, c.project),
    Tagger[PullRequestState, PullRequestEvent](
      _.tags.some,
      {
        case t: PullRequestTagged => Some(tagActive -> t.targetRev)
        case m: PullRequestMerged => Some(tagClosed -> m.rev)
        case _                    => None
      },
      {
        case _: PullRequestMerged => Some(tagActive)
        case _                    => None
      }
    ),
    {
      case s if s.id == id => Some(Set(DependsOn(s.project, id2)))
      case _               => None
    },
    maxDuration,
    xas
  )

  test("Evaluate successfully a command and store both event and state for an initial state") {
    implicit val decoder: Decoder[PullRequestState] = PullRequestState.serializer.codec
    val expectedDependencies                        = Set(DependsOn(proj, id2))
    for {
      _        <- eventLog.evaluate(proj, id, Create(id, proj)).assertEquals((opened, state1))
      _        <- eventStore.history(proj, id).transact(xas.read).assert(opened)
      _        <- eventLog.stateOr(proj, id, NotFound).assertEquals(state1)
      // Check dependency on id2
      _        <- EntityDependencyStore.directDependencies(proj, id, xas).assertEquals(expectedDependencies)
      _        <- EntityDependencyStore.recursiveDependencies(proj, id, xas).assertEquals(expectedDependencies)
      _        <- EntityDependencyStore.decodeDirectDependencies(proj, id, xas).assertEquals(List.empty)
      // Create state for id2
      state1Id2 = state1.copy(id = id2)
      _        <- eventLog.evaluate(proj, id2, Create(id2, proj)).map(_._2).assertEquals(state1Id2)
      _        <- eventLog.stateOr(proj, id2, NotFound).assertEquals(state1Id2)
      _        <- EntityDependencyStore.decodeDirectDependencies(proj, id, xas).assertEquals(List(state1Id2))
    } yield ()
  }

  test("Raise an error with a non-existent project") {
    assertStateNotFound(ProjectRef.unsafe("xxx", "xxx"), id)
  }

  test("Raise an error with a non-existent id") {
    assertStateNotFound(proj, nxv + "xxx")
  }

  test("Tag and check that the state has also been successfully tagged as well") {
    for {
      _ <- eventLog.evaluate(proj, id, TagPR(id, proj, 2, 1)).assertEquals((tagged, state2))
      _ <- eventStore.history(proj, id).transact(xas.read).assert(opened, tagged)
      _ <- eventLog.stateOr(proj, id, NotFound).assertEquals(state2)
      _ <- eventLog.stateOr(proj, id, tagActive, NotFound, TagNotFound).assertEquals(state1)
    } yield ()
  }

  test("Fail to tag when the tagged value can't be replayed up to the target rev") {
    val tagCommand = TagPR(id, proj, 2, 4)
    eventLog.evaluate(proj, id, tagCommand).interceptEquals(EvaluationTagFailure(tagCommand, Some(2)))
  }

  test("Dry run successfully a command without persisting anything") {
    for {
      _ <- eventLog.dryRun(proj, id, Merge(id, proj, 3)).assertEquals((merged, state3))
      _ <- eventStore.history(proj, id).transact(xas.read).assert(opened, tagged)
      _ <- eventLog.stateOr(proj, id, NotFound).assertEquals(state2)
    } yield ()
  }

  test("Evaluate successfully merge command and store both event and state for an initial state") {
    for {
      _ <- eventLog.evaluate(proj, id, Merge(id, proj, 3)).assertEquals((merged, state3))
      _ <- eventStore.history(proj, id).transact(xas.read).assert(opened, tagged, merged)
      _ <- eventLog.stateOr(proj, id, NotFound).assertEquals(state3)
    } yield ()
  }

  test(
    "Check that the state with the active has been successfully removed, the closed one has been set and that a tombstone has been set"
  ) {
    for {
      _ <- eventLog.stateOr(proj, id, tagActive, NotFound, TagNotFound).interceptEquals(TagNotFound)
      _ <- eventLog.stateOr(proj, id, tagClosed, NotFound, TagNotFound).assertEquals(state3)
      _ <- stateTombstoneStore.unsafeGet(proj, id, tagActive).assert(_.isDefined)
    } yield ()
  }

  test("Reject a command and persist nothing") {
    for {
      _ <- eventLog.evaluate(proj, id, Update(id, proj, 3)).interceptEquals(PullRequestAlreadyClosed(id, proj))
      _ <- eventStore.history(proj, id).transact(xas.read).assert(opened, tagged, merged)
      _ <- eventLog.stateOr(proj, id, NotFound).assertEquals(state3)
    } yield ()
  }

  test("Raise an error and persist nothing") {
    val boom = Boom(id, proj, "fail")
    for {
      _ <- eventLog.evaluate(proj, id, boom).intercept[RuntimeException]
      _ <- eventStore.history(proj, id).transact(xas.read).assert(opened, tagged, merged)
      _ <- eventLog.stateOr(proj, id, NotFound).assertEquals(state3)
    } yield ()
  }

  test("Get a timeout and persist nothing") {
    val never   = Never(id, proj)
    val timeout = EvaluationTimeout(never, maxDuration)
    for {
      _ <- eventLog.evaluate(proj, id, never).interceptEquals(timeout)
      _ <- eventStore.history(proj, id).transact(xas.read).assert(opened, tagged, merged)
      _ <- eventLog.stateOr(proj, id, NotFound).assertEquals(state3)
    } yield ()
  }

  test("Get state at the specified revision") {
    eventLog.stateOr(proj, id, 1, NotFound, RevisionNotFound).assertEquals(state1)
  }

  test("Raise an error with a non-existent id") {
    eventLog.stateOr(proj, nxv + "xxx", 1, NotFound, RevisionNotFound).interceptEquals(NotFound)
  }

  test("Raise an error when providing a nonexistent revision") {
    eventLog.stateOr(proj, id, 10, NotFound, RevisionNotFound).interceptEquals(RevisionNotFound(10, 3))
  }

  test("Stream continuously the current states") {
    eventLog
      .states(Scope.root, Offset.Start)
      .assertSize(2)
  }

  test("Delete the entity removes every reference to it and create the tombstones") {
    for {
      _ <- eventLog.delete(proj, id, NotFound)
      // Tagged and latest should be deleted
      _ <- assertStateNotFound(proj, id)
      _ <- eventLog.stateOr(proj, id, tagClosed, NotFound, TagNotFound).interceptEquals(NotFound)
      // Events should be deleted
      _ <- eventStore.history(proj, id).transact(xas.read).assertEmpty
      // Tombstones should be created
      _ <- stateTombstoneStore.unsafeGet(proj, id, tagClosed).assert(_.isDefined)
      _ <- stateTombstoneStore.unsafeGet(proj, id, latest).assert(_.isDefined)
      _ <- eventTombstoneStore.unsafeGet(proj, id).assert(_.isDefined)
      // Dependencies should be deleted
      _ <- EntityDependencyStore.directDependencies(proj, id, xas).assertEquals(Set.empty[DependsOn])
    } yield ()
  }

  test("Delete the entity raises an error for a unknown id") {
    eventLog.delete(proj, nxv + "xxx", NotFound).interceptEquals(NotFound)
  }

}
