package ai.senscience.nexus.delta.sdk.schemas

import ai.senscience.nexus.delta.rdf.Vocabulary.{contexts, nxv, schemas}
import ai.senscience.nexus.delta.rdf.jsonld.api.{JsonLdApi, TitaniumJsonLdApi}
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.shacl.ValidateShacl
import ai.senscience.nexus.delta.sdk.generators.{ProjectGen, SchemaGen}
import ai.senscience.nexus.delta.sdk.schemas.Schemas.*
import ai.senscience.nexus.delta.sdk.schemas.model.SchemaCommand.*
import ai.senscience.nexus.delta.sdk.schemas.model.SchemaEvent.*
import ai.senscience.nexus.delta.sdk.schemas.model.SchemaRejection.*
import ai.senscience.nexus.delta.sdk.schemas.model.{SchemaCommand, SchemaEvent, SchemaState}
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sdk.{organizations, permissions, schemas as schemasModule, RemoteContextResolutionFixtures}
import ai.senscience.nexus.delta.sourcing.model.Identity.User
import ai.senscience.nexus.delta.sourcing.model.Tag.UserTag
import ai.senscience.nexus.delta.sourcing.model.{Label, Tags}
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.effect.IO
import io.circe.Json

import java.time.Instant

class SchemasSuite extends NexusSuite with RemoteContextResolutionFixtures {

  given api: JsonLdApi = TitaniumJsonLdApi.strict

  given rcr: RemoteContextResolution =
    loadCoreContexts(
      schemasModule.contexts.definition ++
        organizations.contexts.definition ++
        permissions.contexts.definition
    )

  private val epoch   = Instant.EPOCH
  private val time2   = Instant.ofEpochMilli(10L)
  private val subject = User("myuser", Label.unsafe("myrealm"))
  private val tag     = UserTag.unsafe("myTag")

  private val project    = ProjectGen.resourceFor(ProjectGen.project("myorg", "myproject", base = nxv.base))
  private val projectRef = project.value.ref

  private val myId      = nxv + "myschema"
  private val source    = jsonContentOf("resources/schema.json").addContext(contexts.shacl, contexts.schemasMetadata)
  private val schema    = SchemaGen.schema(myId, projectRef, source)
  private val compacted = schema.compacted
  private val expanded  = schema.expanded

  private val sourceUpdated = source.replace("targetClass" -> "nxv:Custom", "nxv:Other")
  private val schemaUpdated = SchemaGen.schema(myId, projectRef, sourceUpdated)

  private val currentState = SchemaGen.currentState(schema)

  private val eval: (Option[SchemaState], SchemaCommand) => IO[SchemaEvent] =
    evaluate(ValidateSchema(ValidateShacl(rcr).accepted), clock)

  // -----------------------------------------
  // CreateSchema
  // -----------------------------------------

  test("CreateSchema creates a new event") {
    val command       = CreateSchema(myId, projectRef, source, compacted, expanded, subject)
    val expectedEvent = SchemaCreated(myId, projectRef, source, compacted, expanded, 1, epoch, subject)
    eval(None, command).assertEquals(expectedEvent)
  }

  test("CreateSchema is rejected with InvalidSchema") {
    val wrongSource = source.mapAllKeys("property", _ => Json.obj())
    val wrongSchema = SchemaGen.schema(myId, projectRef, wrongSource)
    val command     = CreateSchema(myId, projectRef, wrongSource, wrongSchema.compacted, wrongSchema.expanded, subject)
    eval(None, command).intercept[InvalidSchema]
  }

  test("CreateSchema is rejected with ResourceAlreadyExists") {
    val command = CreateSchema(myId, projectRef, source, compacted, expanded, subject)
    eval(Some(currentState), command).intercept[ResourceAlreadyExists]
  }

  test("CreateSchema is rejected with ReservedSchemaId") {
    val reserved = schemas + "myid"
    val command  = CreateSchema(reserved, projectRef, source, compacted, expanded, subject)
    eval(None, command).interceptEquals(ReservedSchemaId(reserved))
  }

  // -----------------------------------------
  // UpdateSchema
  // -----------------------------------------

  test("UpdateSchema creates a new event") {
    val updatedCompacted = schemaUpdated.compacted
    val updatedExpanded  = schemaUpdated.expanded
    val command          = UpdateSchema(myId, projectRef, sourceUpdated, updatedCompacted, updatedExpanded, 1, subject)
    val expectedEvent    =
      SchemaUpdated(myId, projectRef, sourceUpdated, updatedCompacted, updatedExpanded, 2, epoch, subject)
    eval(Some(currentState), command).assertEquals(expectedEvent)
  }

  test("UpdateSchema is rejected with InvalidSchema") {
    val wrongSource = source.mapAllKeys("property", _ => Json.obj())
    val wrongSchema = SchemaGen.schema(myId, projectRef, wrongSource)
    val command     = UpdateSchema(myId, projectRef, wrongSource, wrongSchema.compacted, wrongSchema.expanded, 1, subject)
    eval(Some(currentState), command).intercept[InvalidSchema]
  }

  // -----------------------------------------
  // TagSchema
  // -----------------------------------------

  test("TagSchema creates a new event") {
    val state         = SchemaGen.currentState(schema, rev = 2)
    val command       = TagSchema(myId, projectRef, 1, tag, 2, subject)
    val expectedEvent = SchemaTagAdded(myId, projectRef, 1, tag, 3, epoch, subject)
    eval(Some(state), command).assertEquals(expectedEvent)
  }

  test("TagSchema is rejected with RevisionNotFound") {
    val command = TagSchema(myId, projectRef, 3, tag, 1, subject)
    eval(Some(currentState), command).interceptEquals(RevisionNotFound(provided = 3, current = 1))
  }

  // -----------------------------------------
  // DeleteSchemaTag
  // -----------------------------------------

  test("DeleteSchemaTag creates a new event") {
    val state         = SchemaGen.currentState(schema, rev = 2).copy(tags = Tags(tag -> 1))
    val command       = DeleteSchemaTag(myId, projectRef, tag, 2, subject)
    val expectedEvent = SchemaTagDeleted(myId, projectRef, tag, 3, epoch, subject)
    eval(Some(state), command).assertEquals(expectedEvent)
  }

  // -----------------------------------------
  // DeprecateSchema
  // -----------------------------------------

  test("DeprecateSchema creates a new event") {
    val current       = SchemaGen.currentState(schema, rev = 2)
    val command       = DeprecateSchema(myId, projectRef, 2, subject)
    val expectedEvent = SchemaDeprecated(myId, projectRef, 3, epoch, subject)
    eval(Some(current), command).assertEquals(expectedEvent)
  }

  // -----------------------------------------
  // UndeprecateSchema
  // -----------------------------------------

  test("UndeprecateSchema creates a new event") {
    val current       = SchemaGen.currentState(schema, rev = 2, deprecated = true)
    val command       = UndeprecateSchema(myId, projectRef, 2, subject)
    val expectedEvent = SchemaUndeprecated(myId, projectRef, 3, epoch, subject)
    eval(Some(current), command).assertEquals(expectedEvent)
  }

  test("UndeprecateSchema is rejected with SchemaIsNotDeprecated") {
    val current = SchemaGen.currentState(schema)
    val command = UndeprecateSchema(myId, projectRef, 1, subject)
    eval(Some(current), command).intercept[SchemaIsNotDeprecated]
  }

  // -----------------------------------------
  // SchemaNotFound / IncorrectRev common rejections
  // -----------------------------------------

  List(
    "UpdateSchema"      -> UpdateSchema(myId, projectRef, source, compacted, expanded, 2, subject),
    "TagSchema"         -> TagSchema(myId, projectRef, 1, tag, 2, subject),
    "DeleteSchemaTag"   -> DeleteSchemaTag(myId, projectRef, tag, 2, subject),
    "DeprecateSchema"   -> DeprecateSchema(myId, projectRef, 2, subject),
    "UndeprecateSchema" -> UndeprecateSchema(myId, projectRef, 2, subject)
  ).foreach { case (name, command) =>
    test(s"$name is rejected with SchemaNotFound") {
      eval(None, command).intercept[SchemaNotFound]
    }

    test(s"$name is rejected with IncorrectRev") {
      eval(Some(currentState), command).interceptEquals(IncorrectRev(provided = 2, expected = 1))
    }
  }

  // -----------------------------------------
  // SchemaIsDeprecated rejections
  // -----------------------------------------

  private val deprecatedState = SchemaGen.currentState(schema, deprecated = true)

  List(
    "UpdateSchema"    -> UpdateSchema(myId, projectRef, source, compacted, expanded, 1, subject),
    "DeprecateSchema" -> DeprecateSchema(myId, projectRef, 1, subject)
  ).foreach { case (name, command) =>
    test(s"$name is rejected with SchemaIsDeprecated") {
      eval(Some(deprecatedState), command).intercept[SchemaIsDeprecated]
    }
  }

  // -----------------------------------------
  // State transitions (next function)
  // -----------------------------------------

  private val tags    = Tags(UserTag.unsafe("a") -> 1)
  private val current = SchemaGen.currentState(schema.copy(tags = tags))

  test("next: SchemaCreated") {
    val event         = SchemaCreated(myId, projectRef, source, compacted, expanded, 1, epoch, subject)
    val expectedState = current.copy(createdBy = subject, updatedBy = subject, tags = Tags.empty)
    assertEquals(next(None, event), Some(expectedState))
    assertEquals(next(Some(current), event), None)
  }

  test("next: SchemaUpdated") {
    val event         = SchemaUpdated(myId, projectRef, sourceUpdated, compacted, expanded, 2, time2, subject)
    val expectedState = current.copy(rev = 2, source = sourceUpdated, updatedAt = time2, updatedBy = subject)
    assertEquals(next(None, event), None)
    assertEquals(next(Some(current), event), Some(expectedState))
  }

  test("next: SchemaTagAdded") {
    val event         = SchemaTagAdded(myId, projectRef, 1, tag, 2, time2, subject)
    val expectedState = current.copy(rev = 2, updatedAt = time2, updatedBy = subject, tags = tags + (tag -> 1))
    assertEquals(next(None, event), None)
    assertEquals(next(Some(current), event), Some(expectedState))
  }

  test("next: SchemaDeprecated") {
    val event         = SchemaDeprecated(myId, projectRef, 2, time2, subject)
    val expectedState = current.copy(rev = 2, deprecated = true, updatedAt = time2, updatedBy = subject)
    assertEquals(next(None, event), None)
    assertEquals(next(Some(current), event), Some(expectedState))
  }

  test("next: SchemaUndeprecated") {
    val event           = SchemaUndeprecated(myId, projectRef, 2, time2, subject)
    val deprecatedState = current.copy(rev = 2, deprecated = true)
    val expectedState   = current.copy(rev = 2, deprecated = false, updatedAt = time2, updatedBy = subject)
    assertEquals(next(None, event), None)
    assertEquals(next(Some(deprecatedState), event), Some(expectedState))
  }
}
