package ai.senscience.nexus.delta.sdk.resolvers

import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sdk.resolvers.Resolvers.next
import ai.senscience.nexus.delta.sdk.resolvers.model.IdentityResolution.ProvidedIdentities
import ai.senscience.nexus.delta.sdk.resolvers.model.ResolverEvent.{ResolverCreated, ResolverDeprecated, ResolverTagAdded, ResolverUpdated}
import ai.senscience.nexus.delta.sdk.resolvers.model.ResolverValue.{CrossProjectValue, InProjectValue}
import ai.senscience.nexus.delta.sdk.resolvers.model.{Priority, ResolverState, ResolverType}
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.model.Tag.UserTag
import ai.senscience.nexus.testkit.mu.{NexusSuite, OptionAssertions}
import cats.data.NonEmptyList
import io.circe.Json

class ResolversNextSuite extends NexusSuite with ResolverStateMachineFixture with OptionAssertions {

  private val inProjectCreated = ResolverCreated(
    ipId,
    project,
    InProjectValue(Priority.unsafe(22)),
    Json.obj("inProject" -> Json.fromString("created")),
    1,
    epoch,
    bob.subject
  )

  private val crossProjectCreated = ResolverCreated(
    cpId,
    project,
    CrossProjectValue(
      Priority.unsafe(55),
      Set(nxv + "resource"),
      NonEmptyList.of(
        ProjectRef.unsafe("org2", "proj"),
        ProjectRef.unsafe("org2", "proj2")
      ),
      ProvidedIdentities(bob.identities)
    ),
    Json.obj("crossProject" -> Json.fromString("created")),
    1,
    epoch,
    bob.subject
  )

  test("A create event gives a new in-project state from None") {
    val expected = ResolverState(
      ipId,
      project,
      inProjectCreated.value,
      inProjectCreated.source,
      1,
      deprecated = false,
      epoch,
      bob.subject,
      epoch,
      bob.subject
    )
    next(None, inProjectCreated).assertSome(expected)
  }

  test("A create event gives a new cross-project resolver state from None") {
    val expected = ResolverState(
      cpId,
      project,
      crossProjectCreated.value,
      crossProjectCreated.source,
      1,
      deprecated = false,
      epoch,
      bob.subject,
      epoch,
      bob.subject
    )
    next(None, crossProjectCreated).assertSome(expected)
  }

  List(
    inProjectCurrent    -> inProjectCreated,
    crossProjectCurrent -> crossProjectCreated
  ).foreach { case (state, event) =>
    test(s"A create event returns None for an existing ${state.value.tpe} resolver") {
      next(Some(state), event).assertNone()
    }
  }

  val inProjectUpdated = ResolverUpdated(
    ipId,
    project,
    InProjectValue(Priority.unsafe(40)),
    Json.obj("inProject" -> Json.fromString("updated")),
    3,
    instant,
    bob.subject
  )

  val crossCrojectUpdated = ResolverUpdated(
    cpId,
    project,
    CrossProjectValue(
      Priority.unsafe(999),
      Set(nxv + "r", nxv + "r2"),
      NonEmptyList.of(
        ProjectRef.unsafe("org2", "proj"),
        ProjectRef.unsafe("org3", "proj2")
      ),
      ProvidedIdentities(alice.identities)
    ),
    Json.obj("crossProject" -> Json.fromString("updated")),
    3,
    epoch,
    bob.subject
  )

  test("An update event gives a new revision of an existing in-project resolver") {
    val expected = inProjectCurrent.copy(
      value = inProjectUpdated.value,
      source = inProjectUpdated.source,
      rev = inProjectUpdated.rev,
      updatedAt = inProjectUpdated.instant,
      updatedBy = inProjectUpdated.subject
    )
    next(Some(inProjectCurrent), inProjectUpdated).assertSome(expected)
  }

  test("An update event gives a new revision of an existing cross-project resolver") {
    val expected = crossProjectCurrent.copy(
      value = crossCrojectUpdated.value,
      source = crossCrojectUpdated.source,
      rev = crossCrojectUpdated.rev,
      updatedAt = crossCrojectUpdated.instant,
      updatedBy = crossCrojectUpdated.subject
    )
    next(Some(crossProjectCurrent), crossCrojectUpdated).assertSome(expected)
  }

  List(inProjectUpdated, crossCrojectUpdated).foreach { event =>
    test(s"Return None when attempting to update a non-existing ${event.value.tpe} resolver") {
      next(None, event).assertNone()
    }
  }

  List(inProjectCurrent -> crossCrojectUpdated, crossProjectCurrent -> inProjectUpdated).foreach {
    case (state, event) =>
      test(s"Return None when attempting to update an existing ${event.value.tpe} resolver with the other type") {
        next(Some(state), event).assertNone()
      }
  }

  private val tagEvent =
    ResolverTagAdded(ipId, project, ResolverType.InProject, 1, UserTag.unsafe("tag2"), 3, instant, alice.subject)

  bothStates.foreach { state =>
    test(s"Update the tag list fot a ${state.value.tpe} resolver") {
      val expected = state.copy(
        rev = tagEvent.rev,
        updatedAt = tagEvent.instant,
        updatedBy = tagEvent.subject
      )
      next(Some(state), tagEvent).assertSome(expected)
    }
  }

  test(s"Return None when attempting to tag a non-existing resolver") {
    next(None, tagEvent).assertNone()
  }

  private val deprecatedEvent = ResolverDeprecated(ipId, project, ResolverType.InProject, 3, instant, alice.subject)

  bothStates.foreach { state =>
    test(s"mark the current state as deprecated for a ${state.value.tpe} resolver") {
      val expected = state.copy(
        deprecated = true,
        rev = deprecatedEvent.rev,
        updatedAt = deprecatedEvent.instant,
        updatedBy = deprecatedEvent.subject
      )
      next(Some(state), deprecatedEvent).assertSome(expected)
    }
  }

  test(s"Return None when attempting to deprecate a non-existing resolver") {
    next(None, deprecatedEvent).assertNone()
  }

}
