package ai.senscience.nexus.delta.sdk.resources

import ai.senscience.nexus.delta.rdf.Vocabulary.{nxv, schemas}
import ai.senscience.nexus.delta.sdk.generators.ResourceGen
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sdk.jsonld.JsonLdAssembly
import ai.senscience.nexus.delta.sdk.model.jsonld.RemoteContextRef.StaticContextRef
import ai.senscience.nexus.delta.sdk.projects.model.{ApiMappings, ProjectContext}
import ai.senscience.nexus.delta.sdk.resources.Resources.{evaluate, next}
import ai.senscience.nexus.delta.sdk.resources.model.ResourceCommand.*
import ai.senscience.nexus.delta.sdk.resources.model.ResourceEvent.*
import ai.senscience.nexus.delta.sdk.resources.model.ResourceRejection.*
import ai.senscience.nexus.delta.sdk.resources.model.{ResourceCommand, ResourceEvent, ResourceState}
import ai.senscience.nexus.delta.sourcing.model.Identity.User
import ai.senscience.nexus.delta.sourcing.model.ResourceRef.{Latest, Revision}
import ai.senscience.nexus.delta.sourcing.model.Tag.UserTag
import ai.senscience.nexus.delta.sourcing.model.{Label, ResourceRef, Tags}
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.effect.IO
import cats.implicits.*
import io.circe.Json
import io.circe.syntax.KeyOps

import java.time.Instant

class ResourcesSuite extends NexusSuite with ValidateResourceFixture with ResourceInstanceFixture {

  private val epoch   = Instant.EPOCH
  private val time2   = Instant.ofEpochMilli(10L)
  private val subject = User("myuser", Label.unsafe("myrealm"))
  private val caller  = Caller(subject, Set.empty)
  private val tag     = UserTag.unsafe("mytag")

  private val detectChange   = DetectChange(enabled = true)
  private val projectContext = ProjectContext.unsafe(ApiMappings.empty, nxv.base, nxv.base, enforceSchema = false)
  private val jsonld         = JsonLdAssembly(myId, source, compacted, expanded, graph, remoteContexts)

  // Unconstrained schema (schemas.resources)
  private val unconstrainedRef = Latest(schemas.resources)
  private val unconstrainedRev = Revision(schemas.resources, 1)

  // Custom schema
  private val customRef = Latest(nxv + "myschema")
  private val customRev = Revision(customRef.iri, 1)

  private val eval: (Option[ResourceState], ResourceCommand) => IO[ResourceEvent] =
    evaluate(alwaysValidate, detectChange, clock)

  // -----------------------------------------
  // CreateResource
  // -----------------------------------------

  test("CreateResource succeeds with unconstrained schema") {
    eval(None, CreateResource(projectRef, projectContext, unconstrainedRef, jsonld, caller, Some(tag))).assertEquals(
      ResourceCreated(projectRef, unconstrainedRev, projectRef, jsonld, epoch, subject, Some(tag))
    )
  }

  test("CreateResource succeeds with custom schema") {
    eval(None, CreateResource(projectRef, projectContext, customRef, jsonld, caller, Some(tag))).assertEquals(
      ResourceCreated(projectRef, customRev, projectRef, jsonld, epoch, subject, Some(tag))
    )
  }

  // -----------------------------------------
  // UpdateResource
  // -----------------------------------------

  test("UpdateResource succeeds with content change") {
    val updatedSource = source.deepMerge(Json.obj("@type" := types + iri"https://neuroshapes.org/AnotherType"))
    val updatedJsonLd = jsonld.copy(source = updatedSource)
    val current       = ResourceGen.currentState(projectRef, jsonld, unconstrainedRef)
    eval(Some(current), UpdateResource(projectRef, projectContext, None, updatedJsonLd, 1, caller, Some(tag)))
      .assertEquals(
        ResourceUpdated(projectRef, unconstrainedRev, projectRef, updatedJsonLd, 2, epoch, subject, Some(tag))
      )
  }

  test("UpdateResource succeeds with remote context change only") {
    val current       = ResourceGen.currentState(projectRef, jsonld, unconstrainedRef)
    val updatedJsonLd =
      jsonld.copy(remoteContexts = remoteContexts + StaticContextRef(iri"https://senscience.ai/another-context"))
    eval(Some(current), UpdateResource(projectRef, projectContext, None, updatedJsonLd, 1, caller, Some(tag)))
      .assertEquals(
        ResourceUpdated(projectRef, unconstrainedRev, projectRef, updatedJsonLd, 2, epoch, subject, Some(tag))
      )
  }

  test("UpdateResource produces a schema change event when only the schema changes") {
    val current   = ResourceGen.currentState(projectRef, jsonld, unconstrainedRef)
    val newSchema = ResourceRef.Revision(nxv + "new-schema", 1)
    eval(Some(current), UpdateResource(projectRef, projectContext, Some(newSchema), jsonld, 1, caller, Some(tag)))
      .assertEquals(
        ResourceSchemaUpdated(myId, projectRef, newSchema, projectRef, jsonld.types, 2, epoch, subject, Some(tag))
      )
  }

  test("UpdateResource produces a tag event when only a tag is provided") {
    val current = ResourceGen.currentState(projectRef, jsonld, unconstrainedRef)
    eval(Some(current), UpdateResource(projectRef, projectContext, None, jsonld, 1, caller, Some(tag)))
      .assertEquals(ResourceTagAdded(myId, projectRef, jsonld.types, 1, tag, 2, epoch, subject))
  }

  test("UpdateResource is rejected with ResourceNotFound") {
    eval(None, UpdateResource(projectRef, projectContext, None, jsonld, 1, caller, None)).intercept[ResourceNotFound]
  }

  test("UpdateResource is rejected with IncorrectRev") {
    val current = ResourceGen.currentState(projectRef, jsonld)
    eval(Some(current), UpdateResource(projectRef, projectContext, None, jsonld, 2, caller, None))
      .interceptEquals(IncorrectRev(provided = 2, expected = 1))
  }

  test("UpdateResource is rejected with NoChangeDetected") {
    val current = ResourceGen.currentState(projectRef, jsonld, unconstrainedRef)
    eval(Some(current), UpdateResource(projectRef, projectContext, None, jsonld, 1, caller, None))
      .interceptEquals(NoChangeDetected(current))
  }

  test("UpdateResource is rejected with ResourceIsDeprecated") {
    val current = ResourceGen.currentState(projectRef, jsonld, deprecated = true)
    eval(Some(current), UpdateResource(projectRef, projectContext, None, jsonld, 1, caller, None))
      .intercept[ResourceIsDeprecated]
  }

  // -----------------------------------------
  // RefreshResource
  // -----------------------------------------

  test("RefreshResource succeeds on a new remote context") {
    val current       = ResourceGen.currentState(projectRef, jsonld, unconstrainedRef)
    val updatedJsonLd =
      jsonld.copy(remoteContexts = remoteContexts + StaticContextRef(iri"https://senscience.ai/another-context"))
    eval(Some(current), RefreshResource(projectRef, projectContext, None, updatedJsonLd, caller))
      .assertEquals(ResourceRefreshed(projectRef, unconstrainedRev, projectRef, updatedJsonLd, 2, epoch, subject))
  }

  test("RefreshResource is rejected with NoChangeDetected") {
    val current = ResourceGen.currentState(projectRef, jsonld, unconstrainedRef)
    eval(Some(current), RefreshResource(projectRef, projectContext, None, jsonld, caller))
      .interceptEquals(NoChangeDetected(current))
  }

  // -----------------------------------------
  // TagResource
  // -----------------------------------------

  test("TagResource succeeds with unconstrained schema") {
    val current = ResourceGen.currentState(projectRef, jsonld, unconstrainedRef, rev = 2, deprecated = false)
    eval(
      Some(current),
      TagResource(myId, projectRef, None, 1, tag, 2, subject)
    ).assertEquals(
      ResourceTagAdded(myId, projectRef, types, 1, tag, 3, epoch, subject)
    )
  }

  test("TagResource succeeds with custom schema") {
    val current = ResourceGen.currentState(projectRef, jsonld, customRef, rev = 2, deprecated = false)
    eval(
      Some(current),
      TagResource(myId, projectRef, None, 1, tag, 2, subject)
    ).assertEquals(
      ResourceTagAdded(myId, projectRef, types, 1, tag, 3, epoch, subject)
    )
  }

  test("TagResource succeeds with explicit custom schema on deprecated resource") {
    val current = ResourceGen.currentState(projectRef, jsonld, customRef, rev = 2, deprecated = true)
    eval(
      Some(current),
      TagResource(myId, projectRef, Some(customRef), 1, tag, 2, subject)
    ).assertEquals(
      ResourceTagAdded(myId, projectRef, types, 1, tag, 3, epoch, subject)
    )
  }

  test("TagResource is rejected with ResourceNotFound") {
    eval(None, TagResource(myId, projectRef, None, 1, tag, 1, subject))
      .intercept[ResourceNotFound]
  }

  test("TagResource is rejected with IncorrectRev") {
    val current = ResourceGen.currentState(projectRef, jsonld)
    eval(Some(current), TagResource(myId, projectRef, None, 1, tag, 2, subject))
      .interceptEquals(IncorrectRev(provided = 2, expected = 1))
  }

  test("TagResource is rejected with RevisionNotFound") {
    val current = ResourceGen.currentState(projectRef, jsonld)
    eval(
      Some(current),
      TagResource(myId, projectRef, None, 3, tag, 1, subject)
    ).interceptEquals(RevisionNotFound(provided = 3, current = 1))
  }

  // -----------------------------------------
  // DeleteResourceTag
  // -----------------------------------------

  test("DeleteResourceTag succeeds with unconstrained schema") {
    val current = ResourceGen.currentState(projectRef, jsonld, unconstrainedRef, rev = 2, tags = Tags(tag -> 1))
    eval(
      Some(current),
      DeleteResourceTag(myId, projectRef, None, tag, 2, subject)
    ).assertEquals(
      ResourceTagDeleted(myId, projectRef, types, tag, 3, epoch, subject)
    )
  }

  test("DeleteResourceTag succeeds with custom schema") {
    val current = ResourceGen.currentState(projectRef, jsonld, customRef, rev = 2, tags = Tags(tag -> 1))
    eval(
      Some(current),
      DeleteResourceTag(myId, projectRef, None, tag, 2, subject)
    ).assertEquals(
      ResourceTagDeleted(myId, projectRef, types, tag, 3, epoch, subject)
    )
  }

  test("DeleteResourceTag succeeds with explicit custom schema on deprecated resource") {
    val current =
      ResourceGen.currentState(projectRef, jsonld, customRef, rev = 2, deprecated = true, tags = Tags(tag -> 1))
    eval(
      Some(current),
      DeleteResourceTag(myId, projectRef, Some(customRef), tag, 2, subject)
    ).assertEquals(
      ResourceTagDeleted(myId, projectRef, types, tag, 3, epoch, subject)
    )
  }

  test("DeleteResourceTag is rejected with IncorrectRev") {
    val current = ResourceGen.currentState(projectRef, jsonld)
    eval(Some(current), DeleteResourceTag(myId, projectRef, None, tag, 2, subject))
      .interceptEquals(IncorrectRev(provided = 2, expected = 1))
  }

  // -----------------------------------------
  // DeprecateResource
  // -----------------------------------------

  test("DeprecateResource succeeds with unconstrained schema") {
    val current = ResourceGen.currentState(projectRef, jsonld, unconstrainedRef, rev = 2)
    eval(Some(current), DeprecateResource(myId, projectRef, None, 2, subject)).assertEquals(
      ResourceDeprecated(myId, projectRef, types, 3, epoch, subject)
    )
  }

  test("DeprecateResource succeeds with custom schema") {
    val current = ResourceGen.currentState(projectRef, jsonld, customRef, rev = 2)
    eval(Some(current), DeprecateResource(myId, projectRef, None, 2, subject)).assertEquals(
      ResourceDeprecated(myId, projectRef, types, 3, epoch, subject)
    )
  }

  test("DeprecateResource succeeds with explicit custom schema") {
    val current = ResourceGen.currentState(projectRef, jsonld, customRef, rev = 2)
    eval(Some(current), DeprecateResource(myId, projectRef, Some(customRef), 2, subject)).assertEquals(
      ResourceDeprecated(myId, projectRef, types, 3, epoch, subject)
    )
  }

  test("DeprecateResource is rejected with ResourceNotFound") {
    eval(None, DeprecateResource(myId, projectRef, None, 1, subject))
      .intercept[ResourceNotFound]
  }

  test("DeprecateResource is rejected with IncorrectRev") {
    val current = ResourceGen.currentState(projectRef, jsonld)
    eval(Some(current), DeprecateResource(myId, projectRef, None, 2, subject))
      .interceptEquals(IncorrectRev(provided = 2, expected = 1))
  }

  test("DeprecateResource is rejected with UnexpectedResourceSchema") {
    val current = ResourceGen.currentState(projectRef, jsonld, customRef, rev = 2)
    eval(current.some, DeprecateResource(myId, projectRef, unconstrainedRef.some, 2, subject))
      .intercept[UnexpectedResourceSchema]
  }

  test("DeprecateResource is rejected with ResourceIsDeprecated") {
    val current = ResourceGen.currentState(projectRef, jsonld, deprecated = true)
    eval(Some(current), DeprecateResource(myId, projectRef, None, 1, subject))
      .intercept[ResourceIsDeprecated]
  }

  // -----------------------------------------
  // UndeprecateResource
  // -----------------------------------------

  test("UndeprecateResource succeeds") {
    val deprecatedState = ResourceGen.currentState(projectRef, jsonld, deprecated = true)
    eval(deprecatedState.some, UndeprecateResource(myId, projectRef, None, 1, subject)).assertEquals(
      ResourceUndeprecated(myId, projectRef, types, 2, epoch, subject)
    )
  }

  test("UndeprecateResource is rejected with ResourceNotFound") {
    eval(None, UndeprecateResource(myId, projectRef, None, 1, subject))
      .intercept[ResourceNotFound]
  }

  test("UndeprecateResource is rejected with IncorrectRev") {
    val current = ResourceGen.currentState(projectRef, jsonld)
    eval(Some(current), UndeprecateResource(myId, projectRef, None, 2, subject))
      .interceptEquals(IncorrectRev(provided = 2, expected = 1))
  }

  test("UndeprecateResource is rejected with UnexpectedResourceSchema") {
    val current = ResourceGen.currentState(projectRef, jsonld, customRef, rev = 2)
    eval(current.some, UndeprecateResource(myId, projectRef, unconstrainedRef.some, 2, subject))
      .intercept[UnexpectedResourceSchema]
  }

  test("UndeprecateResource is rejected with ResourceIsNotDeprecated") {
    val activeState = ResourceGen.currentState(projectRef, jsonld)
    eval(activeState.some, UndeprecateResource(myId, projectRef, None, 1, subject)).intercept[ResourceIsNotDeprecated]
  }

  // -----------------------------------------
  // State transitions (next function)
  // -----------------------------------------

  private val tags           = Tags(tag -> 1)
  private val currentState   = ResourceGen.currentState(projectRef, jsonld, unconstrainedRef, tags)
  private val stateCompacted = currentState.compacted
  private val stateExpanded  = currentState.expanded
  private val stateContexts  = currentState.remoteContexts

  test("next: ResourceCreated from None produces a new state") {
    val result = next(
      None,
      ResourceCreated(
        myId,
        projectRef,
        unconstrainedRev,
        projectRef,
        types,
        source,
        stateCompacted,
        stateExpanded,
        stateContexts,
        1,
        epoch,
        subject,
        Some(tag)
      )
    )
    assertEquals(
      result,
      Some(
        currentState.copy(
          createdAt = epoch,
          schema = unconstrainedRev,
          createdBy = subject,
          updatedAt = epoch,
          updatedBy = subject,
          tags = Tags(tag -> unconstrainedRev.rev)
        )
      )
    )
  }

  test("next: ResourceCreated from existing state returns None") {
    val result = next(
      Some(currentState),
      ResourceCreated(
        myId,
        projectRef,
        unconstrainedRev,
        projectRef,
        types,
        source,
        stateCompacted,
        stateExpanded,
        stateContexts,
        1,
        time2,
        subject,
        Some(tag)
      )
    )
    assertEquals(result, None)
  }

  test("next: ResourceUpdated from None returns None") {
    val newTypes = types + (nxv + "Other")
    val result   = next(
      None,
      ResourceUpdated(
        myId,
        projectRef,
        unconstrainedRev,
        projectRef,
        newTypes,
        source,
        stateCompacted,
        stateExpanded,
        stateContexts,
        1,
        time2,
        subject,
        None
      )
    )
    assertEquals(result, None)
  }

  test("next: ResourceUpdated from existing state produces a new state") {
    val newTypes  = types + (nxv + "Other")
    val newSource = source.deepMerge(Json.obj("key" := "value"))
    val result    = next(
      Some(currentState),
      ResourceUpdated(
        myId,
        projectRef,
        unconstrainedRev,
        projectRef,
        newTypes,
        newSource,
        stateCompacted,
        stateExpanded,
        stateContexts,
        2,
        time2,
        subject,
        None
      )
    )
    assertEquals(
      result,
      Some(
        currentState.copy(
          rev = 2,
          source = newSource,
          updatedAt = time2,
          updatedBy = subject,
          types = newTypes,
          schema = unconstrainedRev
        )
      )
    )
  }

  test("next: ResourceTagAdded from None returns None") {
    val result = next(None, ResourceTagAdded(myId, projectRef, types, 1, tag, 2, time2, subject))
    assertEquals(result, None)
  }

  test("next: ResourceTagAdded from existing state produces a new state") {
    val anotherTag = UserTag.unsafe("another")
    val result     = next(Some(currentState), ResourceTagAdded(myId, projectRef, types, 1, anotherTag, 2, time2, subject))
    assertEquals(
      result,
      Some(currentState.copy(rev = 2, updatedAt = time2, updatedBy = subject, tags = tags + (anotherTag -> 1)))
    )
  }

  test("next: ResourceDeprecated from None returns None") {
    val result = next(None, ResourceDeprecated(myId, projectRef, types, 1, time2, subject))
    assertEquals(result, None)
  }

  test("next: ResourceDeprecated from existing state produces a new state") {
    val result = next(Some(currentState), ResourceDeprecated(myId, projectRef, types, 2, time2, subject))
    assertEquals(
      result,
      Some(currentState.copy(rev = 2, deprecated = true, updatedAt = time2, updatedBy = subject))
    )
  }

  test("next: ResourceUndeprecated from None returns None") {
    val resourceUndeprecatedEvent = ResourceUndeprecated(myId, projectRef, types, 2, time2, subject)
    val result                    = next(None, resourceUndeprecatedEvent)
    assertEquals(result, None)
  }

  test("next: ResourceUndeprecated from deprecated state produces a new state") {
    val resourceUndeprecatedEvent = ResourceUndeprecated(myId, projectRef, types, 2, time2, subject)
    val deprecatedState           = currentState.copy(deprecated = true)
    val result                    = next(deprecatedState.some, resourceUndeprecatedEvent)
    assertEquals(
      result,
      Some(deprecatedState.copy(rev = 2, deprecated = false, updatedAt = time2, updatedBy = subject))
    )
  }
}
