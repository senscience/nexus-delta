package ai.senscience.nexus.delta.sdk.resolvers

import ai.senscience.nexus.delta.sdk.Defaults
import ai.senscience.nexus.delta.sdk.error.ServiceError.ScopeInitializationFailed
import ai.senscience.nexus.delta.sdk.generators.ProjectGen
import ai.senscience.nexus.delta.sdk.resolvers.model.ResolverRejection.ResourceAlreadyExists
import ai.senscience.nexus.delta.sdk.resolvers.model.ResolverValue.InProjectValue
import ai.senscience.nexus.delta.sdk.resolvers.model.{Priority, ResolverValue}
import ai.senscience.nexus.delta.sourcing.model.Identity.{Subject, User}
import ai.senscience.nexus.delta.sourcing.model.Label
import cats.effect.{IO, Ref}
import ch.epfl.bluebrain.nexus.delta.rdf.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.testkit.mu.NexusSuite

class ResolverScopeInitializationSuite extends NexusSuite {

  private val defaults = Defaults("resolverName", "resolverDescription")

  private val project = ProjectGen.project("org", "project")

  private val usersRealm: Label = Label.unsafe("users")
  private val bob: Subject      = User("bob", usersRealm)

  test("Succeeds") {
    for {
      ref      <- Ref.of[IO, Option[ResolverValue]](None)
      scopeInit = new ResolverScopeInitialization(
                    (_, resolver) => ref.set(Some(resolver)),
                    defaults
                  )
      _        <- scopeInit.onProjectCreation(project.ref, bob)
      expected  = InProjectValue(Some(defaults.name), Some(defaults.description), Priority.unsafe(1))
      _        <- ref.get.assertEquals(Some(expected))
    } yield ()
  }

  test("Recovers if the resolver already exists") {
    val scopeInit = new ResolverScopeInitialization(
      (project, _) => IO.raiseError(ResourceAlreadyExists(nxv.defaultResolver, project)),
      defaults
    )
    scopeInit.onProjectCreation(project.ref, bob).assert
  }

  test("Raises a failure otherwise") {
    val scopeInit = new ResolverScopeInitialization(
      (_, _) => IO.raiseError(new IllegalStateException("Something got wrong !")),
      defaults
    )
    scopeInit.onProjectCreation(project.ref, bob).intercept[ScopeInitializationFailed]
  }
}
