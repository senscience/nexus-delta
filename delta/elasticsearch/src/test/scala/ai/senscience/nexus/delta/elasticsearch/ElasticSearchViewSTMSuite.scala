package ai.senscience.nexus.delta.elasticsearch

import ai.senscience.nexus.delta.elasticsearch.ElasticSearchViews.{evaluate, next}
import ai.senscience.nexus.delta.elasticsearch.model.ElasticSearchViewCommand.*
import ai.senscience.nexus.delta.elasticsearch.model.ElasticSearchViewEvent.*
import ai.senscience.nexus.delta.elasticsearch.model.ElasticSearchViewRejection.*
import ai.senscience.nexus.delta.elasticsearch.model.ElasticSearchViewType.ElasticSearch as ElasticSearchType
import ai.senscience.nexus.delta.elasticsearch.model.ElasticSearchViewValue.*
import ai.senscience.nexus.delta.elasticsearch.model.{ElasticSearchViewState, ElasticSearchViewValue}
import ai.senscience.nexus.delta.kernel.utils.UUIDF
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.syntax.*
import ai.senscience.nexus.delta.sdk.permissions.model.Permission
import ai.senscience.nexus.delta.sdk.views.{IndexingRev, ViewRef}
import ai.senscience.nexus.delta.sourcing.model.Identity.{Anonymous, Subject, User}
import ai.senscience.nexus.delta.sourcing.model.Tag.UserTag
import ai.senscience.nexus.delta.sourcing.model.{Label, ProjectRef, Tags}
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.data.NonEmptySet
import cats.effect.IO
import io.circe.Json

import java.time.Instant
import java.util.UUID

class ElasticSearchViewSTMSuite extends NexusSuite {

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

  // format: off
  private val indexingValue            =
    IndexingElasticSearchViewValue(None, List(), None, None, None, Permission.unsafe("my/permission"))
  private val indexingValueWithUserTag = indexingValue.copy(resourceTag = Some(UserTag.unsafe("tag")))
  private val indexingValueWithName    = indexingValue.copy(name = Some("viewName"))
  private val aggregateValue           = AggregateElasticSearchViewValue(NonEmptySet.of(viewRef))
  // format: on

  private val invalidView: ValidateElasticSearchView =
    (_: UUID, _: IndexingRev, _: ElasticSearchViewValue) => IO.raiseError(InvalidElasticSearchIndexPayload(None))

  private def current(
      id: Iri = id,
      project: ProjectRef = project,
      uuid: UUID = uuid,
      value: ElasticSearchViewValue = indexingValue,
      source: Json = source,
      tags: Tags = Tags.empty,
      rev: Int = 1,
      indexingRev: IndexingRev = IndexingRev.init,
      deprecated: Boolean = false,
      createdAt: Instant = epoch,
      createdBy: Subject = Anonymous,
      updatedAt: Instant = epoch,
      updatedBy: Subject = Anonymous
  ): ElasticSearchViewState =
    ElasticSearchViewState(
      id,
      project,
      uuid,
      value,
      source,
      tags,
      rev,
      indexingRev,
      deprecated,
      createdAt,
      createdBy,
      updatedAt,
      updatedBy
    )

  private val eval = evaluate(ValidateElasticSearchView.always, clock)(_, _)

  // CreateElasticSearchView

  test("CreateElasticSearchView succeeds for an IndexingElasticSearchViewValue") {
    val cmd      = CreateElasticSearchView(id, project, indexingValue, source, subject)
    val expected = ElasticSearchViewCreated(id, project, uuid, indexingValue, source, 1, epoch, subject)
    eval(None, cmd).assertEquals(expected)
  }

  test("CreateElasticSearchView succeeds for an AggregateElasticSearchViewValue") {
    val cmd      = CreateElasticSearchView(id, project, aggregateValue, source, subject)
    val expected = ElasticSearchViewCreated(id, project, uuid, aggregateValue, source, 1, epoch, subject)
    eval(None, cmd).assertEquals(expected)
  }

  test("CreateElasticSearchView raises ResourceAlreadyExists when view already exists") {
    val cmd = CreateElasticSearchView(id, project, aggregateValue, source, subject)
    eval(Some(current()), cmd).intercept[ResourceAlreadyExists]
  }

  test("CreateElasticSearchView raises InvalidElasticSearchIndexPayload for invalid mapping") {
    val cmd = CreateElasticSearchView(id, project, indexingValue, source, subject)
    evaluate(invalidView, clock)(None, cmd).intercept[InvalidElasticSearchIndexPayload]
  }

  // UpdateElasticSearchView

  test("UpdateElasticSearchView succeeds for an IndexingElasticSearchViewValue") {
    val value    = indexingValue.copy(resourceTag = Some(UserTag.unsafe("sometag")))
    val cmd      = UpdateElasticSearchView(id, project, 1, value, source, subject)
    val expected = ElasticSearchViewUpdated(id, project, uuid, value, source, 2, epoch, subject)
    eval(Some(current()), cmd).assertEquals(expected)
  }

  test("UpdateElasticSearchView succeeds for an AggregateElasticSearchViewValue") {
    val state    =
      current(value = aggregateValue.copy(views = NonEmptySet.of(ViewRef(project, iri"http://localhost/view"))))
    val cmd      = UpdateElasticSearchView(id, project, 1, aggregateValue, source, subject)
    val expected = ElasticSearchViewUpdated(id, project, uuid, aggregateValue, source, 2, epoch, subject)
    eval(Some(state), cmd).assertEquals(expected)
  }

  test("UpdateElasticSearchView raises ViewNotFound when view doesn't exist") {
    val cmd = UpdateElasticSearchView(id, project, 1, indexingValue, source, subject)
    eval(None, cmd).intercept[ViewNotFound]
  }

  test("UpdateElasticSearchView raises IncorrectRev for wrong revision") {
    val cmd = UpdateElasticSearchView(id, project, 2, indexingValue, source, subject)
    eval(Some(current()), cmd).intercept[IncorrectRev]
  }

  test("UpdateElasticSearchView raises ViewIsDeprecated for deprecated view") {
    val cmd = UpdateElasticSearchView(id, project, 1, indexingValue, source, subject)
    eval(Some(current(deprecated = true)), cmd).intercept[ViewIsDeprecated]
  }

  test("UpdateElasticSearchView raises DifferentElasticSearchViewType for mismatched type") {
    val cmd = UpdateElasticSearchView(id, project, 1, aggregateValue, source, subject)
    eval(Some(current()), cmd).intercept[DifferentElasticSearchViewType]
  }

  test("UpdateElasticSearchView raises InvalidElasticSearchIndexPayload for invalid mapping") {
    val cmd = UpdateElasticSearchView(id, project, 1, indexingValue, source, subject)
    evaluate(invalidView, clock)(Some(current()), cmd).intercept[InvalidElasticSearchIndexPayload]
  }

  // DeprecateElasticSearchView

  test("DeprecateElasticSearchView succeeds") {
    val cmd      = DeprecateElasticSearchView(id, project, 1, subject)
    val expected = ElasticSearchViewDeprecated(id, project, ElasticSearchType, uuid, 2, epoch, subject)
    eval(Some(current()), cmd).assertEquals(expected)
  }

  test("DeprecateElasticSearchView raises ViewNotFound when view doesn't exist") {
    val cmd = DeprecateElasticSearchView(id, project, 1, subject)
    eval(None, cmd).intercept[ViewNotFound]
  }

  test("DeprecateElasticSearchView raises IncorrectRev for wrong revision") {
    val cmd = DeprecateElasticSearchView(id, project, 2, subject)
    eval(Some(current()), cmd).intercept[IncorrectRev]
  }

  test("DeprecateElasticSearchView raises ViewIsDeprecated for deprecated view") {
    val cmd = DeprecateElasticSearchView(id, project, 1, subject)
    eval(Some(current(deprecated = true)), cmd).intercept[ViewIsDeprecated]
  }

  // UndeprecateElasticSearchView

  test("UndeprecateElasticSearchView succeeds") {
    val cmd      = UndeprecateElasticSearchView(id, project, 1, subject)
    val expected = ElasticSearchViewUndeprecated(id, project, ElasticSearchType, uuid, 2, epoch, subject)
    eval(Some(current(deprecated = true)), cmd).assertEquals(expected)
  }

  test("UndeprecateElasticSearchView raises ViewNotFound when view doesn't exist") {
    val cmd = UndeprecateElasticSearchView(id, project, 1, subject)
    eval(None, cmd).intercept[ViewNotFound]
  }

  test("UndeprecateElasticSearchView raises IncorrectRev for wrong revision") {
    val cmd = UndeprecateElasticSearchView(id, project, 2, subject)
    eval(Some(current(deprecated = true)), cmd).intercept[IncorrectRev]
  }

  test("UndeprecateElasticSearchView raises ViewIsNotDeprecated for active view") {
    val cmd = UndeprecateElasticSearchView(id, project, 1, subject)
    eval(Some(current()), cmd).intercept[ViewIsNotDeprecated]
  }

  // next (pure state transitions)

  test("ElasticSearchViewCreated event is discarded for a Current state") {
    val event = ElasticSearchViewCreated(id, project, uuid, indexingValue, source, 1, epoch, subject)
    assertEquals(next(Some(current()), event), None)
  }

  test("ElasticSearchViewCreated event changes the state") {
    val event    = ElasticSearchViewCreated(id, project, uuid, aggregateValue, source, 1, epoch, subject)
    val expected = current(value = aggregateValue, createdBy = subject, updatedBy = subject)
    assertEquals(next(None, event), Some(expected))
  }

  test("ElasticSearchViewUpdated event is discarded for an Initial state") {
    val event = ElasticSearchViewUpdated(id, project, uuid, indexingValue, source, 2, epoch, subject)
    assertEquals(next(None, event), None)
  }

  test("ElasticSearchViewUpdated event changes the state (aggregate view)") {
    val event    = ElasticSearchViewUpdated(id, project, uuid, aggregateValue, source2, 2, epochPlus10, subject)
    val expected =
      current(value = aggregateValue, source = source2, rev = 2, updatedAt = epochPlus10, updatedBy = subject)
    assertEquals(next(Some(current()), event), Some(expected))
  }

  test("ElasticSearchViewUpdated event changes the state (indexing view, indexing revision)") {
    val event    = ElasticSearchViewUpdated(id, project, uuid, indexingValueWithUserTag, source2, 2, epochPlus10, subject)
    val expected = current(
      value = indexingValueWithUserTag,
      source = source2,
      rev = 2,
      indexingRev = IndexingRev(2),
      updatedAt = epochPlus10,
      updatedBy = subject
    )
    assertEquals(next(Some(current()), event), Some(expected))
  }

  test("ElasticSearchViewUpdated event changes the state (indexing view, non-indexing revision)") {
    val event    = ElasticSearchViewUpdated(id, project, uuid, indexingValueWithName, source2, 2, epochPlus10, subject)
    val expected = current(
      value = indexingValueWithName,
      source = source2,
      rev = 2,
      updatedAt = epochPlus10,
      updatedBy = subject
    )
    assertEquals(next(Some(current()), event), Some(expected))
  }

  test("ElasticSearchViewTagAdded event is discarded for an Initial state") {
    val tag   = UserTag.unsafe("tag")
    val event = ElasticSearchViewTagAdded(id, project, ElasticSearchType, uuid, 1, tag, 2, epoch, subject)
    assertEquals(next(None, event), None)
  }

  test("ElasticSearchViewTagAdded event changes the state") {
    val tag      = UserTag.unsafe("tag")
    val event    = ElasticSearchViewTagAdded(id, project, ElasticSearchType, uuid, 1, tag, 2, epoch, subject)
    val expected = current(tags = Tags(tag -> 1), rev = 2, updatedBy = subject)
    assertEquals(next(Some(current()), event), Some(expected))
  }

  test("ElasticSearchViewDeprecated event is discarded for an Initial state") {
    val event = ElasticSearchViewDeprecated(id, project, ElasticSearchType, uuid, 2, epoch, subject)
    assertEquals(next(None, event), None)
  }

  test("ElasticSearchViewDeprecated event changes the state") {
    val event    = ElasticSearchViewDeprecated(id, project, ElasticSearchType, uuid, 2, epoch, subject)
    val expected = current(deprecated = true, rev = 2, updatedBy = subject)
    assertEquals(next(Some(current()), event), Some(expected))
  }

  test("ElasticSearchViewUndeprecated event is discarded for an Initial state") {
    val event = ElasticSearchViewUndeprecated(id, project, ElasticSearchType, uuid, 2, epoch, subject)
    assertEquals(next(None, event), None)
  }

  test("ElasticSearchViewUndeprecated event changes the state") {
    val event    = ElasticSearchViewUndeprecated(id, project, ElasticSearchType, uuid, 2, epoch, subject)
    val expected = current(deprecated = false, rev = 2, updatedBy = subject)
    assertEquals(next(Some(current(deprecated = true)), event), Some(expected))
  }
}
