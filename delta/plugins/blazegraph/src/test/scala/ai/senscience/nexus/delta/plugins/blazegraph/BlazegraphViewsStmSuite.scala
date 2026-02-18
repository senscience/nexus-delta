package ai.senscience.nexus.delta.plugins.blazegraph

import ai.senscience.nexus.delta.kernel.utils.UUIDF
import ai.senscience.nexus.delta.plugins.blazegraph.BlazegraphViews.{evaluate, next}
import ai.senscience.nexus.delta.plugins.blazegraph.model.BlazegraphViewCommand.*
import ai.senscience.nexus.delta.plugins.blazegraph.model.BlazegraphViewEvent.*
import ai.senscience.nexus.delta.plugins.blazegraph.model.BlazegraphViewRejection.*
import ai.senscience.nexus.delta.plugins.blazegraph.model.BlazegraphViewType.IndexingBlazegraphView as BlazegraphType
import ai.senscience.nexus.delta.plugins.blazegraph.model.BlazegraphViewValue.{AggregateBlazegraphViewValue, IndexingBlazegraphViewValue}
import ai.senscience.nexus.delta.plugins.blazegraph.model.{BlazegraphViewState, BlazegraphViewValue}
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.syntax.*
import ai.senscience.nexus.delta.sdk.permissions.model.Permission
import ai.senscience.nexus.delta.sdk.views.ViewRef
import ai.senscience.nexus.delta.sourcing.model.Identity.{Anonymous, Subject, User}
import ai.senscience.nexus.delta.sourcing.model.Tag.UserTag
import ai.senscience.nexus.delta.sourcing.model.{IriFilter, Label, ProjectRef}
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.data.NonEmptySet
import cats.effect.IO
import io.circe.Json

import java.time.Instant
import java.util.UUID

class BlazegraphViewsStmSuite extends NexusSuite with Fixtures {

  private val uuid    = UUID.randomUUID()
  private given UUIDF = UUIDF.fixed(uuid)

  private val epoch       = Instant.EPOCH
  private val epochPlus10 = Instant.EPOCH.plusMillis(10L)
  private val subject     = User("myuser", Label.unsafe("myrealm"))
  private val id          = iri"http://localhost/${UUID.randomUUID()}"
  private val project     = ProjectRef(Label.unsafe("myorg"), Label.unsafe("myproj"))
  private val viewRef     = ViewRef(project, iri"http://localhost/${UUID.randomUUID()}")
  private val source      = Json.obj()
  private val source2     = Json.obj("key" -> Json.fromInt(1))

  private val indexingValue            = IndexingBlazegraphViewValue(
    None,
    None,
    IriFilter.None,
    IriFilter.None,
    None,
    includeMetadata = false,
    includeDeprecated = false,
    Permission.unsafe("my/permission")
  )
  private val tag: UserTag             = UserTag.unsafe("tag")
  private val indexingValueWithUserTag = indexingValue.copy(resourceTag = Some(tag))
  private val indexingValueWithName    = indexingValue.copy(name = Some("name"))
  private val aggregateValue           = AggregateBlazegraphViewValue(None, None, NonEmptySet.of(viewRef))

  private def current(
      id: Iri = id,
      project: ProjectRef = project,
      uuid: UUID = uuid,
      value: BlazegraphViewValue = indexingValue,
      source: Json = source,
      rev: Int = 1,
      indexingRev: Int = 1,
      deprecated: Boolean = false,
      createdAt: Instant = epoch,
      createdBy: Subject = Anonymous,
      updatedAt: Instant = epoch,
      updatedBy: Subject = Anonymous
  ): BlazegraphViewState =
    BlazegraphViewState(
      id,
      project,
      uuid,
      value,
      source,
      rev,
      indexingRev,
      deprecated,
      createdAt,
      createdBy,
      updatedAt,
      updatedBy
    )

  private val invalidView: ValidateBlazegraphView = {
    case v: AggregateBlazegraphViewValue => IO.raiseError(InvalidViewReferences(v.views.toSortedSet))
    case v: IndexingBlazegraphViewValue  => IO.raiseError(PermissionIsNotDefined(v.permission))
  }

  private val eval = evaluate(alwaysValidate, clock)(_, _)

  // Evaluating CreateBlazegraphView

  test("Create emits BlazegraphViewCreated for an IndexingBlazegraphViewValue") {
    val cmd      = CreateBlazegraphView(id, project, indexingValue, source, subject)
    val expected = BlazegraphViewCreated(id, project, uuid, indexingValue, source, 1, epoch, subject)
    eval(None, cmd).assertEquals(expected)
  }

  test("Create emits BlazegraphViewCreated for an AggregateBlazegraphViewValue") {
    val cmd      = CreateBlazegraphView(id, project, aggregateValue, source, subject)
    val expected = BlazegraphViewCreated(id, project, uuid, aggregateValue, source, 1, epoch, subject)
    eval(None, cmd).assertEquals(expected)
  }

  test("Create raises ResourceAlreadyExists when view already exists") {
    val cmd = CreateBlazegraphView(id, project, aggregateValue, source, subject)
    eval(Some(current()), cmd).intercept[ResourceAlreadyExists]
  }

  test("Create raises PermissionIsNotDefined") {
    val cmd = CreateBlazegraphView(id, project, indexingValue, source, subject)
    evaluate(invalidView, clock)(None, cmd).intercept[PermissionIsNotDefined]
  }

  // Evaluating UpdateBlazegraphView

  test("Update emits BlazegraphViewUpdated for an IndexingBlazegraphViewValue") {
    val value    = indexingValue.copy(resourceTag = Some(UserTag.unsafe("sometag")))
    val cmd      = UpdateBlazegraphView(id, project, value, 1, source, subject)
    val expected = BlazegraphViewUpdated(id, project, uuid, value, source, 2, epoch, subject)
    eval(Some(current()), cmd).assertEquals(expected)
  }

  test("Update emits BlazegraphViewUpdated for an AggregateBlazegraphViewValue") {
    val state    =
      current(value = aggregateValue.copy(views = NonEmptySet.of(ViewRef(project, iri"http://localhost/view"))))
    val cmd      = UpdateBlazegraphView(id, project, aggregateValue, 1, source, subject)
    val expected = BlazegraphViewUpdated(id, project, uuid, aggregateValue, source, 2, epoch, subject)
    eval(Some(state), cmd).assertEquals(expected)
  }

  test("Update raises ViewNotFound") {
    val cmd = UpdateBlazegraphView(id, project, indexingValue, 1, source, subject)
    eval(None, cmd).intercept[ViewNotFound]
  }

  test("Update raises IncorrectRev") {
    val cmd = UpdateBlazegraphView(id, project, indexingValue, 2, source, subject)
    eval(Some(current()), cmd).intercept[IncorrectRev]
  }

  test("Update raises ViewIsDeprecated") {
    val cmd = UpdateBlazegraphView(id, project, indexingValue, 1, source, subject)
    eval(Some(current(deprecated = true)), cmd).intercept[ViewIsDeprecated]
  }

  test("Update raises DifferentBlazegraphViewType") {
    val cmd = UpdateBlazegraphView(id, project, aggregateValue, 1, source, subject)
    eval(Some(current()), cmd).intercept[DifferentBlazegraphViewType]
  }

  test("Update raises InvalidViewReferences") {
    val cmd = UpdateBlazegraphView(id, project, aggregateValue, 1, source, subject)
    evaluate(invalidView, clock)(Some(current(value = aggregateValue)), cmd).intercept[InvalidViewReferences]
  }

  test("Update raises PermissionIsNotDefined") {
    val cmd = UpdateBlazegraphView(id, project, indexingValue, 1, source, subject)
    evaluate(invalidView, clock)(Some(current()), cmd).intercept[PermissionIsNotDefined]
  }

  // Evaluating DeprecateBlazegraphView

  test("Deprecate emits BlazegraphViewDeprecated") {
    val cmd      = DeprecateBlazegraphView(id, project, 1, subject)
    val expected = BlazegraphViewDeprecated(id, project, BlazegraphType, uuid, 2, epoch, subject)
    eval(Some(current()), cmd).assertEquals(expected)
  }

  test("Deprecate raises ViewNotFound") {
    val cmd = DeprecateBlazegraphView(id, project, 1, subject)
    eval(None, cmd).intercept[ViewNotFound]
  }

  test("Deprecate raises IncorrectRev") {
    val cmd = DeprecateBlazegraphView(id, project, 2, subject)
    eval(Some(current()), cmd).intercept[IncorrectRev]
  }

  test("Deprecate raises ViewIsDeprecated") {
    val cmd = DeprecateBlazegraphView(id, project, 1, subject)
    eval(Some(current(deprecated = true)), cmd).intercept[ViewIsDeprecated]
  }

  // Evaluating UndeprecateBlazegraphView

  test("Undeprecate emits BlazegraphViewUndeprecated") {
    val deprecatedState   = Some(current(deprecated = true))
    val undeprecateCmd    = UndeprecateBlazegraphView(id, project, 1, subject)
    val undeprecatedEvent = BlazegraphViewUndeprecated(id, project, BlazegraphType, uuid, 2, epoch, subject)
    eval(deprecatedState, undeprecateCmd).assertEquals(undeprecatedEvent)
  }

  test("Undeprecate raises ViewNotFound") {
    val undeprecateCmd = UndeprecateBlazegraphView(id, project, 1, subject)
    eval(None, undeprecateCmd).intercept[ViewNotFound]
  }

  test("Undeprecate raises IncorrectRev") {
    val deprecatedState = Some(current(deprecated = true))
    val undeprecateCmd  = UndeprecateBlazegraphView(id, project, 2, subject)
    eval(deprecatedState, undeprecateCmd).intercept[IncorrectRev]
  }

  test("Undeprecate raises ViewIsNotDeprecated") {
    val undeprecateCmd = UndeprecateBlazegraphView(id, project, 1, subject)
    eval(Some(current()), undeprecateCmd).intercept[ViewIsNotDeprecated]
  }

  // Applying BlazegraphViewCreated event

  test("BlazegraphViewCreated is discarded for a Current state") {
    val event = BlazegraphViewCreated(id, project, uuid, indexingValue, source, 1, epoch, subject)
    assertEquals(next(Some(current()), event), None)
  }

  test("BlazegraphViewCreated changes the state") {
    val event    = BlazegraphViewCreated(id, project, uuid, aggregateValue, source, 1, epoch, subject)
    val expected = current(value = aggregateValue, createdBy = subject, updatedBy = subject)
    assertEquals(next(None, event), Some(expected))
  }

  // Applying BlazegraphViewUpdated event

  test("BlazegraphViewUpdated is discarded for an Initial state") {
    val event = BlazegraphViewUpdated(id, project, uuid, indexingValue, source, 2, epoch, subject)
    assertEquals(next(None, event), None)
  }

  test("BlazegraphViewUpdated changes the state (aggregate view)") {
    val event    = BlazegraphViewUpdated(id, project, uuid, aggregateValue, source2, 2, epochPlus10, subject)
    val expected =
      current(value = aggregateValue, source = source2, rev = 2, updatedAt = epochPlus10, updatedBy = subject)
    assertEquals(next(Some(current()), event), Some(expected))
  }

  test("BlazegraphViewUpdated changes the state (indexing view, indexing revision)") {
    val event    = BlazegraphViewUpdated(id, project, uuid, indexingValueWithUserTag, source2, 2, epochPlus10, subject)
    val expected =
      current(
        value = indexingValueWithUserTag,
        source = source2,
        rev = 2,
        indexingRev = 2,
        updatedAt = epochPlus10,
        updatedBy = subject
      )
    assertEquals(next(Some(current()), event), Some(expected))
  }

  test("BlazegraphViewUpdated changes the state (indexing view, non-indexing revision)") {
    val event    = BlazegraphViewUpdated(id, project, uuid, indexingValueWithName, source2, 2, epochPlus10, subject)
    val expected =
      current(value = indexingValueWithName, source = source2, rev = 2, updatedAt = epochPlus10, updatedBy = subject)
    assertEquals(next(Some(current()), event), Some(expected))
  }

  test("Applying BlazegraphViewTagAdded event") {
    val event = BlazegraphViewTagAdded(id, project, BlazegraphType, uuid, 1, tag, 2, epoch, subject)
    assertEquals(next(None, event), None)

    val expected = current(rev = 2, updatedBy = subject)
    assertEquals(next(Some(current()), event), Some(expected))
  }

  test("Applying BlazegraphViewDeprecated event") {
    val event = BlazegraphViewDeprecated(id, project, BlazegraphType, uuid, 2, epoch, subject)
    assertEquals(next(None, event), None)

    val expected = current(deprecated = true, rev = 2, updatedBy = subject)
    assertEquals(next(Some(current()), event), Some(expected))
  }

  test("Applying BlazegraphViewUndeprecated event") {
    val event = BlazegraphViewUndeprecated(id, project, BlazegraphType, uuid, 2, epoch, subject)
    assertEquals(next(None, event), None)

    val initial  = current().copy(deprecated = true)
    val expected = current(deprecated = false, rev = 2, updatedBy = subject)
    assertEquals(next(Some(initial), event), Some(expected))
  }

}
