package ai.senscience.nexus.delta.sdk.resolvers

import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.resolvers.model.IdentityResolution.ProvidedIdentities
import ai.senscience.nexus.delta.sdk.resolvers.model.ResolverValue.{CrossProjectValue, InProjectValue}
import ai.senscience.nexus.delta.sdk.resolvers.model.{Priority, ResolverState}
import ai.senscience.nexus.delta.sourcing.model.Identity.{Anonymous, Authenticated, Group, User}
import ai.senscience.nexus.delta.sourcing.model.{Label, ProjectRef}
import cats.data.NonEmptyList
import io.circe.Json

import java.time.Instant
trait ResolverStateMachineFixture {

  val epoch   = Instant.EPOCH
  val instant = Instant.ofEpochMilli(1000L)
  val realm   = Label.unsafe("myrealm")
  val bob     = Caller(User("Bob", realm), Set(User("Bob", realm), Group("mygroup", realm), Authenticated(realm)))
  val alice   = Caller(User("Alice", realm), Set(User("Alice", realm), Group("mygroup2", realm)))

  val project  = ProjectRef.unsafe("org", "proj")
  val priority = Priority.unsafe(42)

  val ipId = nxv + "in-project"
  val cpId = nxv + "cross-project"

  val inProjectCurrent = ResolverState(
    ipId,
    project,
    InProjectValue(priority),
    Json.obj(),
    2,
    deprecated = false,
    epoch,
    bob.subject,
    instant,
    Anonymous
  )

  val crossProjectCurrent = ResolverState(
    cpId,
    project,
    CrossProjectValue(
      priority,
      Set.empty,
      NonEmptyList.of(
        ProjectRef.unsafe("org2", "proj")
      ),
      ProvidedIdentities(bob.identities)
    ),
    Json.obj(),
    2,
    deprecated = false,
    epoch,
    alice.subject,
    instant,
    bob.subject
  )

  val bothStates = List(inProjectCurrent, crossProjectCurrent)

}
