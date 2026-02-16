package ai.senscience.nexus.delta.sdk.resolvers

import ai.senscience.nexus.delta.kernel.utils.UUIDF
import ai.senscience.nexus.delta.rdf.Vocabulary.{contexts, nxv, schema}
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.sdk.generators.ProjectGen
import ai.senscience.nexus.delta.sdk.generators.ResolverGen.{resolverResourceFor, sourceFrom, sourceWithoutId}
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.implicits.{*, given}
import ai.senscience.nexus.delta.sdk.jsonld.JsonLdRejection.{DecodingFailed, UnexpectedId}
import ai.senscience.nexus.delta.sdk.model.IdSegmentRef
import ai.senscience.nexus.delta.sdk.projects.{FetchContextDummy, Projects}
import ai.senscience.nexus.delta.sdk.projects.model.ApiMappings
import ai.senscience.nexus.delta.sdk.projects.model.ProjectRejection.{ProjectIsDeprecated, ProjectNotFound}
import ai.senscience.nexus.delta.sdk.resolvers.model.*
import ai.senscience.nexus.delta.sdk.resolvers.model.IdentityResolution.{ProvidedIdentities, UseCurrentCaller}
import ai.senscience.nexus.delta.sdk.resolvers.model.ResolverRejection.*
import ai.senscience.nexus.delta.sdk.resolvers.model.ResolverValue.{CrossProjectValue, InProjectValue}
import ai.senscience.nexus.delta.sdk.resources.Resources
import ai.senscience.nexus.delta.sdk.{ConfigFixtures, ResolverResource}
import ai.senscience.nexus.delta.sourcing.EntityDependencyStore
import ai.senscience.nexus.delta.sourcing.model.EntityDependency.DependsOn
import ai.senscience.nexus.delta.sourcing.model.Identity.{Authenticated, Group, Subject, User}
import ai.senscience.nexus.delta.sourcing.model.{Label, ProjectRef}
import ai.senscience.nexus.delta.sourcing.postgres.Doobie
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.data.NonEmptyList
import cats.effect.IO
import munit.AnyFixture

import java.util.UUID

class ResolversImplSuite extends NexusSuite with Doobie.Fixture with ConfigFixtures {

  override def munitFixtures: Seq[AnyFixture[?]] = List(doobie)

  private val realm = Label.unsafe("myrealm")

  private given bobSubject: Subject = User("Bob", realm)
  private given bob: Caller         = Caller(bobSubject, Set(User("Bob", realm), Group("mygroup", realm), Authenticated(realm)))
  private val alice                 = Caller(User("Alice", realm), Set(User("Alice", realm), Group("mygroup2", realm)))

  private val uuid    = UUID.randomUUID()
  private given UUIDF = UUIDF.fixed(uuid)

  private def res: RemoteContextResolution =
    RemoteContextResolution.fixed(
      contexts.resolvers         -> jsonContentOf("contexts/resolvers.json").topContextValueOrEmpty,
      contexts.resolversMetadata -> jsonContentOf("contexts/resolvers-metadata.json").topContextValueOrEmpty
    )

  private val resolverContextResolution: ResolverContextResolution = ResolverContextResolution(res)

  private val org               = Label.unsafe("org")
  private val apiMappings       = ApiMappings("nxv" -> nxv.base, "Person" -> schema.Person)
  private val base              = nxv.base
  private val project           = ProjectGen.project("org", "proj", base = base, mappings = apiMappings + Resources.mappings)
  private val deprecatedProject = ProjectGen.project("org", "proj-deprecated")

  private val projectRef           = project.ref
  private val deprecatedProjectRef = deprecatedProject.ref
  private val unknownProjectRef    = ProjectRef(org, Label.unsafe("xxx"))
  private val referencedProject    = ProjectRef.unsafe("org", "proj2")

  private val inProjectPrio    = Priority.unsafe(42)
  private val crossProjectPrio = Priority.unsafe(43)

  private val fetchContext = FetchContextDummy(
    Map(project.ref -> project.context, deprecatedProject.ref -> deprecatedProject.context),
    Set(deprecatedProject.ref)
  )

  private lazy val xas = doobie()

  private lazy val validatePriority = ValidatePriority.priorityAlreadyExists(xas)

  private lazy val resolvers: Resolvers = ResolversImpl(
    fetchContext,
    resolverContextResolution,
    validatePriority,
    eventLogConfig,
    xas,
    clock
  )

  private val inProjectValue = InProjectValue(inProjectPrio)

  private val updatedInProjectValue = InProjectValue(Priority.unsafe(99))

  private val crossProjectValue = CrossProjectValue(
    crossProjectPrio,
    Set.empty,
    NonEmptyList.of(referencedProject),
    ProvidedIdentities(bob.identities)
  )

  private val updatedCrossProjectValue = crossProjectValue.copy(identityResolution = UseCurrentCaller)

  // Creating resolvers

  test("Creating an in-project resolver succeeds with id as segment") {
    val payload  = sourceWithoutId(inProjectValue)
    val expected = resolverResourceFor(nxv + "in-project", projectRef, inProjectValue, payload, subject = bob.subject)
    resolvers.create(nxv + "in-project", projectRef, payload).assertEquals(expected)
  }

  test("Creating a cross-project resolver succeeds with id as segment") {
    val payload  = sourceWithoutId(crossProjectValue)
    val expected =
      resolverResourceFor(nxv + "cross-project", projectRef, crossProjectValue, payload, subject = bob.subject)
    resolvers.create(nxv + "cross-project", projectRef, payload).assertEquals(expected)
  }

  test("Creating a cross-project resolver saves the dependency to the referenced project") {
    val expected = Set(DependsOn(referencedProject, Projects.encodeId(referencedProject)))
    EntityDependencyStore.directDependencies(projectRef, nxv + "cross-project", xas).assertEquals(expected)
  }

  test("Creating an in-project resolver succeeds with id only in payload") {
    val id       = nxv + "in-project-payload"
    val value    = inProjectValue.copy(priority = Priority.unsafe(44))
    val payload  = sourceFrom(id, value)
    val expected = resolverResourceFor(id, projectRef, value, payload, subject = bob.subject)
    resolvers.create(projectRef, payload).assertEquals(expected)
  }

  test("Creating a cross-project resolver succeeds with id only in payload") {
    val id       = nxv + "cross-project-payload"
    val value    = crossProjectValue.copy(priority = Priority.unsafe(45))
    val payload  = sourceFrom(id, value)
    val expected = resolverResourceFor(id, projectRef, value, payload, subject = bob.subject)
    resolvers.create(projectRef, payload).assertEquals(expected)
  }

  test("Creating an in-project resolver succeeds with same id in segment and payload") {
    val id       = nxv + "in-project-both"
    val value    = inProjectValue.copy(priority = Priority.unsafe(46))
    val payload  = sourceFrom(id, value)
    val expected = resolverResourceFor(id, projectRef, value, payload, subject = alice.subject)
    resolvers.create(id, projectRef, payload)(using alice).assertEquals(expected)
  }

  test("Creating a cross-project resolver succeeds with same id in segment and payload") {
    val id       = nxv + "cross-project-both"
    val value    = crossProjectValue.copy(identityResolution = UseCurrentCaller, priority = Priority.unsafe(47))
    val payload  = sourceFrom(id, value)
    val expected = resolverResourceFor(id, projectRef, value, payload, subject = alice.subject)
    resolvers.create(id, projectRef, payload)(using alice).assertEquals(expected)
  }

  test("Creating a resolver succeeds with a generated id") {
    val expectedId    = nxv.base / uuid.toString
    val expectedValue = crossProjectValue.copy(resourceTypes = Set(nxv.Schema), priority = Priority.unsafe(48))
    val payload       = sourceWithoutId(expectedValue)
    val expected      = resolverResourceFor(expectedId, projectRef, expectedValue, payload, subject = bob.subject)
    resolvers.create(projectRef, payload).assertEquals(expected)
  }

  test("Creating an in-project resolver succeeds with a parsed value") {
    val id       = nxv + "in-project-from-value"
    val value    = inProjectValue.copy(priority = Priority.unsafe(49))
    val expected =
      resolverResourceFor(id, projectRef, value, ResolverValue.generateSource(id, value), subject = bob.subject)
    resolvers.create(id, projectRef, value).assertEquals(expected)
  }

  test("Creating a cross-project resolver succeeds with a parsed value") {
    val id       = nxv + "cross-project-from-value"
    val value    = crossProjectValue.copy(priority = Priority.unsafe(50))
    val expected =
      resolverResourceFor(id, projectRef, value, ResolverValue.generateSource(id, value), subject = bob.subject)
    resolvers.create(id, projectRef, value).assertEquals(expected)
  }

  test("Creating a resolver fails if ids in segment and payload are different") {
    val payloadId = nxv + "resolver-fail"
    val id        = nxv + "in-project"
    val payload   = sourceFrom(payloadId, inProjectValue)
    resolvers.create(id, projectRef, payload).interceptEquals(UnexpectedId(id, payloadId))
  }

  test("Creating a resolver fails if id is not valid") {
    val payload = sourceWithoutId(inProjectValue)
    resolvers.create("{a@*", projectRef, payload).interceptEquals(InvalidResolverId("{a@*"))
  }

  test("Creating a resolver fails if priority already exists") {
    resolvers
      .create(nxv + "in-project-other", projectRef, inProjectValue)
      .interceptEquals(PriorityAlreadyExists(projectRef, nxv + "in-project", inProjectValue.priority))
  }

  test("Creating a resolver fails if it already exists") {
    val newPrio = Priority.unsafe(51)
    val payload = sourceWithoutId(inProjectValue.copy(priority = newPrio))
    resolvers
      .create((nxv + "in-project").toString, projectRef, payload)
      .interceptEquals(ResourceAlreadyExists(nxv + "in-project", projectRef))
  }

  test("Creating a resolver with payload id fails if it already exists") {
    val newPrio       = Priority.unsafe(51)
    val payloadWithId = sourceFrom(nxv + "in-project", inProjectValue.copy(priority = newPrio))
    resolvers.create(projectRef, payloadWithId).interceptEquals(ResourceAlreadyExists(nxv + "in-project", projectRef))
  }

  test("Creating a resolver fails if the project does not exist") {
    val payload = sourceWithoutId(inProjectValue)
    resolvers.create(nxv + "in-project", unknownProjectRef, payload).intercept[ProjectNotFound]
  }

  test("Creating a resolver with payload id fails if the project does not exist") {
    val payloadWithId = sourceFrom(nxv + "in-project", inProjectValue)
    resolvers.create(unknownProjectRef, payloadWithId).intercept[ProjectNotFound]
  }

  test("Creating a resolver fails if the project is deprecated") {
    val payload = sourceWithoutId(inProjectValue)
    resolvers.create(nxv + "in-project", deprecatedProjectRef, payload).intercept[ProjectIsDeprecated]
  }

  test("Creating a resolver with payload id fails if the project is deprecated") {
    val payloadWithId = sourceFrom(nxv + "in-project", inProjectValue)
    resolvers.create(deprecatedProjectRef, payloadWithId).intercept[ProjectIsDeprecated]
  }

  test("Creating a cross-project resolver fails if no identities are provided") {
    val invalidValue =
      crossProjectValue.copy(identityResolution = ProvidedIdentities(Set.empty), priority = Priority.unsafe(52))
    val payload      = sourceWithoutId(invalidValue)
    resolvers.create(nxv + "cross-project-no-id", projectRef, payload).interceptEquals(NoIdentities)
  }

  test("Creating a cross-project resolver fails if some identities don't belong to the caller") {
    val identities   = ProvidedIdentities(Set(bob.subject, alice.subject))
    val invalidValue = crossProjectValue.copy(identityResolution = identities, priority = Priority.unsafe(53))
    val payload      = sourceWithoutId(invalidValue)
    resolvers
      .create(nxv + "cross-project-miss-id", projectRef, payload)
      .interceptEquals(InvalidIdentities(Set(alice.subject)))
  }

  test("Creating a cross-project resolver fails if mandatory values in source are missing") {
    val payload = sourceWithoutId(crossProjectValue).removeKeys("projects")
    resolvers
      .create(nxv + "cross-project-miss-fields", projectRef, payload)
      .intercept[DecodingFailed]
  }

  // Updating resolvers

  test("Updating an in-project resolver succeeds with id as segment") {
    val payload  = sourceWithoutId(updatedInProjectValue)
    val expected = resolverResourceFor(
      nxv + "in-project",
      projectRef,
      updatedInProjectValue,
      payload,
      rev = 2,
      subject = bob.subject
    )
    resolvers.update(nxv + "in-project", projectRef, 1, payload).assertEquals(expected)
  }

  test("Updating a cross-project resolver succeeds with id as segment") {
    val payload  = sourceWithoutId(updatedCrossProjectValue)
    val expected = resolverResourceFor(
      nxv + "cross-project",
      projectRef,
      updatedCrossProjectValue,
      payload,
      rev = 2,
      subject = bob.subject
    )
    resolvers.update(nxv + "cross-project", projectRef, 1, payload).assertEquals(expected)
  }

  test("Updating an in-project resolver succeeds with a parsed value") {
    val id       = nxv + "in-project-from-value"
    val value    = inProjectValue.copy(priority = Priority.unsafe(999))
    val expected = resolverResourceFor(
      id,
      projectRef,
      value,
      ResolverValue.generateSource(id, value),
      rev = 2,
      subject = bob.subject
    )
    resolvers.update(id, projectRef, 1, value).assertEquals(expected)
  }

  test("Updating a cross-project resolver succeeds with a parsed value") {
    val id       = nxv + "cross-project-from-value"
    val value    = crossProjectValue.copy(priority = Priority.unsafe(998))
    val expected = resolverResourceFor(
      id,
      projectRef,
      value,
      ResolverValue.generateSource(id, value),
      rev = 2,
      subject = bob.subject
    )
    resolvers.update(id, projectRef, 1, value).assertEquals(expected)
  }

  test("Updating a resolver fails if it doesn't exist") {
    val payload = sourceWithoutId(inProjectValue.copy(priority = Priority.unsafe(54)))
    resolvers
      .update(nxv + "in-project-xxx", projectRef, 1, payload)
      .interceptEquals(ResolverNotFound(nxv + "in-project-xxx", projectRef))
  }

  test("Updating a resolver fails if the revision does not match") {
    val payload = sourceWithoutId(inProjectValue)
    resolvers
      .update(nxv + "in-project", projectRef, 5, payload)
      .interceptEquals(IncorrectRev(5, 2))
  }

  test("Updating a resolver fails if ids in segment and payload are different") {
    val payloadId = nxv + "resolver-fail"
    val payload   = sourceFrom(payloadId, inProjectValue)
    resolvers
      .update(nxv + "in-project", projectRef, 2, payload)
      .interceptEquals(UnexpectedId(id = nxv + "in-project", payloadId = payloadId))
  }

  test("Updating a resolver fails if the project does not exist") {
    val payload = sourceWithoutId(inProjectValue)
    resolvers.update(nxv + "in-project", unknownProjectRef, 2, payload).intercept[ProjectNotFound]
  }

  test("Updating a resolver fails if the project is deprecated") {
    val payload = sourceWithoutId(inProjectValue)
    resolvers.update(nxv + "in-project", deprecatedProjectRef, 2, payload).intercept[ProjectIsDeprecated]
  }

  test("Updating a cross-project resolver fails if no identities are provided") {
    val invalidValue = crossProjectValue.copy(identityResolution = ProvidedIdentities(Set.empty))
    val payload      = sourceWithoutId(invalidValue)
    resolvers.update(nxv + "cross-project", projectRef, 2, payload).interceptEquals(NoIdentities)
  }

  test("Updating a cross-project resolver fails if some identities don't belong to the caller") {
    val identities   = ProvidedIdentities(Set(bob.subject, alice.subject))
    val invalidValue = crossProjectValue.copy(identityResolution = identities, priority = Priority.unsafe(55))
    val payload      = sourceWithoutId(invalidValue)
    resolvers
      .update(nxv + "cross-project", projectRef, 2, payload)
      .interceptEquals(InvalidIdentities(Set(alice.subject)))
  }

  // Deprecating resolvers

  test("Deprecating an in-project resolver succeeds") {
    val expected = resolverResourceFor(
      nxv + "in-project",
      projectRef,
      updatedInProjectValue,
      sourceWithoutId(updatedInProjectValue),
      rev = 3,
      subject = bob.subject,
      deprecated = true
    )
    resolvers.deprecate(nxv + "in-project", projectRef, 2).assertEquals(expected)
  }

  test("Deprecating a cross-project resolver succeeds") {
    val expected = resolverResourceFor(
      nxv + "cross-project",
      projectRef,
      updatedCrossProjectValue,
      sourceWithoutId(updatedCrossProjectValue),
      rev = 3,
      subject = bob.subject,
      deprecated = true
    )
    resolvers.deprecate(nxv + "cross-project", projectRef, 2).assertEquals(expected)
  }

  test("Deprecating a resolver fails if it doesn't exist") {
    resolvers
      .deprecate(nxv + "in-project-xxx", projectRef, 3)
      .interceptEquals(ResolverNotFound(nxv + "in-project-xxx", projectRef))
  }

  test("Deprecating a resolver fails if the revision does not match") {
    resolvers
      .deprecate(nxv + "in-project", projectRef, 10)
      .interceptEquals(IncorrectRev(10, 3))
  }

  test("Deprecating a resolver fails if the project does not exist") {
    resolvers.deprecate(nxv + "in-project", unknownProjectRef, 3).intercept[ProjectNotFound]
  }

  test("Deprecating a resolver fails if the project is deprecated") {
    resolvers.deprecate(nxv + "in-project", deprecatedProjectRef, 3).intercept[ProjectIsDeprecated]
  }

  test("Deprecating a resolver fails if it is already deprecated") {
    resolvers
      .deprecate(nxv + "in-project", projectRef, 3)
      .interceptEquals(ResolverIsDeprecated(nxv + "in-project"))
  }

  test("Updating a deprecated resolver fails") {
    resolvers
      .update(nxv + "in-project", projectRef, 3, sourceWithoutId(inProjectValue))
      .interceptEquals(ResolverIsDeprecated(nxv + "in-project"))
  }

  // Fetching resolvers

  private val inProjectExpected = resolverResourceFor(
    nxv + "in-project",
    projectRef,
    updatedInProjectValue,
    sourceWithoutId(updatedInProjectValue),
    rev = 3,
    subject = bob.subject,
    deprecated = true
  )

  private val crossProjectExpected = resolverResourceFor(
    nxv + "cross-project",
    projectRef,
    updatedCrossProjectValue,
    sourceWithoutId(updatedCrossProjectValue),
    rev = 3,
    subject = bob.subject,
    deprecated = true
  )

  List(inProjectExpected, crossProjectExpected).foreach { resolver =>
    val resolverId   = resolver.value.id
    val resolverType = resolver.value.value.tpe
    test(s"Fetching a '$resolverType' resolver succeeds") {
      resolvers.fetch(resolverId.toString, projectRef).assertEquals(resolver)
    }

    test(s"Fetching a '$resolverType' resolver by rev succeeds") {
      resolvers.fetch(IdSegmentRef(resolverId, 3), projectRef).assertEquals(resolver)
    }
  }

  test("Fetching a resolver fails if it does not exist") {
    resolvers.fetch("xxx", projectRef).intercept[ResolverNotFound]
  }

  test("Fetching a resolver at a specific rev fails if it does not exist") {
    resolvers.fetch(IdSegmentRef("xxx", 1), projectRef).intercept[ResolverNotFound]
  }

  test("Fetching a resolver fails if the revision does not exist") {
    resolvers.fetch(IdSegmentRef(nxv + "in-project", 30), projectRef).interceptEquals(RevisionNotFound(30, 3))
  }

  // Listing resolvers

  test("Listing resolvers returns all resolvers in the project") {
    resolvers.list(projectRef).map(_.total).assertEquals(9L)
  }

  // Priority validation

  test("Creating a resolver with the same priority as a deprecated resolver succeeds") {
    givenADeprecatedInProjectResolver(666) { resolver =>
      resolvers.create(nxv + genString(), resolver.value.project, InProjectValue(resolver.value.priority))
    }
  }

  private def givenADeprecatedInProjectResolver(priority: Int)(test: ResolverResource => IO[ResolverResource]) = {
    val id            = nxv + genString()
    val resolverValue = InProjectValue(Priority.unsafe(priority))
    for {
      _          <- resolvers.create(id, projectRef, resolverValue)
      deprecated <- resolvers.deprecate(id, projectRef, 1)
      _          <- test(deprecated)
    } yield ()
  }
}
