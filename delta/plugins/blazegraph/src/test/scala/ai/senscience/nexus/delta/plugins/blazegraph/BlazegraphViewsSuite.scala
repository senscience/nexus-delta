package ai.senscience.nexus.delta.plugins.blazegraph

import ai.senscience.nexus.delta.kernel.utils.UUIDF
import ai.senscience.nexus.delta.plugins.blazegraph.BlazegraphViewsGen.resourceFor
import ai.senscience.nexus.delta.plugins.blazegraph.model.*
import ai.senscience.nexus.delta.plugins.blazegraph.model.BlazegraphViewRejection.*
import ai.senscience.nexus.delta.plugins.blazegraph.model.BlazegraphViewValue.*
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sdk.ConfigFixtures
import ai.senscience.nexus.delta.sdk.generators.ProjectGen
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.model.*
import ai.senscience.nexus.delta.sdk.permissions.model.Permission
import ai.senscience.nexus.delta.sdk.projects.FetchContextDummy
import ai.senscience.nexus.delta.sdk.projects.model.ApiMappings
import ai.senscience.nexus.delta.sdk.projects.model.ProjectRejection.{ProjectIsDeprecated, ProjectNotFound}
import ai.senscience.nexus.delta.sdk.resolvers.ResolverContextResolution
import ai.senscience.nexus.delta.sdk.views.ViewRef
import ai.senscience.nexus.delta.sourcing.model.Identity.{Authenticated, Group, User}
import ai.senscience.nexus.delta.sourcing.model.Tag.UserTag
import ai.senscience.nexus.delta.sourcing.model.{IriFilter, Label, ProjectRef}
import ai.senscience.nexus.delta.sourcing.postgres.Doobie
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.data.NonEmptySet
import cats.effect.IO
import io.circe.Json
import munit.{AnyFixture, Location}

import java.util.UUID

class BlazegraphViewsSuite extends NexusSuite with Doobie.Fixture with ConfigFixtures with Fixtures {

  override def munitFixtures: Seq[AnyFixture[?]] = List(doobie)

  private val uuid           = UUID.randomUUID()
  private given uuidF: UUIDF = UUIDF.fixed(uuid)

  private val prefix = "prefix"

  private val realm            = Label.unsafe("myrealm")
  private given bob: User      = User("Bob", realm)
  private given caller: Caller = Caller(bob, Set(bob, Group("mygroup", realm), Authenticated(realm)))

  private val indexingValue  = IndexingBlazegraphViewValue(
    None,
    None,
    IriFilter.None,
    IriFilter.None,
    None,
    includeMetadata = false,
    includeDeprecated = false,
    permissions.query
  )
  private val indexingSource = jsonContentOf("indexing-view-source.json")

  private val updatedIndexingValue  = indexingValue.copy(resourceTag = Some(UserTag.unsafe("v1.5")))
  private val updatedIndexingSource = indexingSource.mapObject(_.add("resourceTag", Json.fromString("v1.5")))

  private val indexingViewId  = nxv + "indexing-view"
  private val indexingViewId2 = nxv + "indexing-view3"

  private val undefinedPermission = Permission.unsafe("not/defined")

  private val base              = nxv.base
  private val project           = ProjectGen.project("org", "proj", base = base, mappings = ApiMappings.empty)
  private val deprecatedProject = ProjectGen.project("org", "proj-deprecated")
  private val projectRef        = project.ref

  private val viewRef         = ViewRef(project.ref, indexingViewId)
  private val aggregateValue  = AggregateBlazegraphViewValue(None, None, NonEmptySet.of(viewRef))
  private val aggregateViewId = nxv + "aggregate-view"
  private val aggregateSource = jsonContentOf("aggregate-view-source.json")

  private val tag = UserTag.unsafe("v1.5")

  private val doesntExistId = nxv + "doesntexist"

  private val fetchContext = FetchContextDummy(Map(project.ref -> project.context), Set(deprecatedProject.ref))

  private lazy val views: BlazegraphViews = BlazegraphViews(
    fetchContext,
    ResolverContextResolution(rcr),
    ValidateBlazegraphView(
      IO.pure(Set(permissions.query)),
      2,
      doobie()
    ),
    _ => IO.unit,
    eventLogConfig,
    prefix,
    doobie(),
    clock
  ).accepted

  private def assertDeprecated(view: ViewResource)(using Location): Unit    = assert(view.deprecated)
  private def assertNotDeprecated(view: ViewResource)(using Location): Unit = assert(!view.deprecated)

  // Creating a view

  test("Create rejects when referenced view does not exist") {
    val expectedError = InvalidViewReferences(Set(viewRef))
    views.create(aggregateViewId, projectRef, aggregateValue).interceptEquals(expectedError)
  }

  test("Create an IndexingBlazegraphView") {
    val expected =
      resourceFor(indexingViewId, projectRef, indexingValue, uuid, indexingSource, createdBy = bob, updatedBy = bob)
    views.create(indexingViewId, projectRef, indexingValue).assertEquals(expected)
  }

  test("Create an AggregateBlazegraphViewValue") {
    val expected =
      resourceFor(aggregateViewId, projectRef, aggregateValue, uuid, aggregateSource, createdBy = bob, updatedBy = bob)
    views.create(aggregateViewId, projectRef, aggregateValue).assertEquals(expected)
  }

  test("Create rejects when the project does not exist") {
    val nonExistent = ProjectRef.unsafe("org", "nonexistent")
    views.create(indexingViewId, nonExistent, indexingValue).intercept[ProjectNotFound]
  }

  test("Create rejects when the project is deprecated") {
    views.create(indexingViewId, deprecatedProject.ref, indexingValue).intercept[ProjectIsDeprecated]
  }

  test("Create rejects when view already exists") {
    val expectedError = ResourceAlreadyExists(aggregateViewId, projectRef)
    views.create(aggregateViewId, projectRef, aggregateValue).interceptEquals(expectedError)
  }

  test("Create rejects when the permission is not defined") {
    val withUnknownPermission = indexingValue.copy(permission = undefinedPermission)
    val expectedError         = PermissionIsNotDefined(undefinedPermission)
    views.create(indexingViewId2, projectRef, withUnknownPermission).interceptEquals(expectedError)
  }

  // Updating a view

  test("Update an IndexingBlazegraphView") {
    val expected = resourceFor(
      indexingViewId,
      projectRef,
      updatedIndexingValue,
      uuid,
      updatedIndexingSource,
      2,
      indexingRev = 2,
      createdBy = bob,
      updatedBy = bob
    )
    views.update(indexingViewId, projectRef, 1, updatedIndexingValue).assertEquals(expected)
  }

  test("Update an AggregateBlazegraphView") {
    val expected = resourceFor(
      aggregateViewId,
      projectRef,
      aggregateValue,
      uuid,
      aggregateSource,
      2,
      createdBy = bob,
      updatedBy = bob
    )
    views.update(aggregateViewId, projectRef, 1, aggregateValue).assertEquals(expected)
  }

  test("Update rejects when view doesn't exist") {
    val expectedError = ViewNotFound(indexingViewId2, projectRef)
    views.update(indexingViewId2, projectRef, 1, indexingValue).interceptEquals(expectedError)
  }

  test("Update rejects when incorrect revision is provided") {
    views.update(indexingViewId, projectRef, 1, indexingValue).interceptEquals(IncorrectRev(1, 2))
  }

  test("Update rejects when trying to change the view type") {
    val expectedError = DifferentBlazegraphViewType(
      indexingViewId,
      BlazegraphViewType.AggregateBlazegraphView,
      BlazegraphViewType.IndexingBlazegraphView
    )
    views.update(indexingViewId, projectRef, 2, aggregateValue).interceptEquals(expectedError)
  }

  test("Update rejects when referenced view does not exist") {
    val nonExistentViewRef            = ViewRef(projectRef, indexingViewId2)
    val aggregateValueWithInvalidView = AggregateBlazegraphViewValue(None, None, NonEmptySet.of(nonExistentViewRef))
    val expectedError                 = InvalidViewReferences(Set(nonExistentViewRef))
    views.update(aggregateViewId, projectRef, 2, aggregateValueWithInvalidView).interceptEquals(expectedError)
  }

  test("Update rejects when view is deprecated") {
    val expectedError = ViewIsDeprecated(indexingViewId2)
    views.create(indexingViewId2, projectRef, indexingValue) >>
      views.deprecate(indexingViewId2, projectRef, 1) >>
      views.update(indexingViewId2, projectRef, 2, indexingValue).interceptEquals(expectedError)
  }

  test("Update rejects when referenced view is deprecated") {
    val deprecatedViewRef             = ViewRef(projectRef, indexingViewId2)
    val aggregateValueWithInvalidView = AggregateBlazegraphViewValue(None, None, NonEmptySet.of(deprecatedViewRef))
    val expectedError                 = InvalidViewReferences(Set(deprecatedViewRef))
    views.update(aggregateViewId, projectRef, 2, aggregateValueWithInvalidView).interceptEquals(expectedError)
  }

  test("Update rejects when the permission is not defined") {
    val withUnknownPermission = indexingValue.copy(permission = undefinedPermission)
    val expectedError         = PermissionIsNotDefined(undefinedPermission)
    views.update(indexingViewId, projectRef, 2, withUnknownPermission).interceptEquals(expectedError)
  }

  // Deprecating a view

  test("Deprecate the view") {
    views.deprecate(aggregateViewId, projectRef, 2).map(assertDeprecated) >>
      views.fetch(aggregateViewId, projectRef).map(assertDeprecated)
  }

  test("Deprecate rejects when view doesn't exist") {
    val doesntExist = nxv + "doesntexist"
    views.deprecate(doesntExist, projectRef, 1).interceptEquals(ViewNotFound(doesntExist, projectRef))
  }

  test("Deprecate rejects when incorrect revision is provided") {
    val viewId = genString()
    for {
      _ <- views.create(viewId, projectRef, indexingValue)
      _ <- views.deprecate(viewId, projectRef, 42).interceptEquals(IncorrectRev(42, 1))
      r <- views.fetch(viewId, projectRef)
      _  = assertNotDeprecated(r)
    } yield ()
  }

  // Undeprecating a view

  test("Undeprecate the view") {
    val viewId = genString()
    for {
      _ <- views.create(viewId, projectRef, indexingValue)
      _ <- views.deprecate(viewId, projectRef, 1)
      r <- views.undeprecate(viewId, projectRef, 2)
      _  = assertNotDeprecated(r)
      f <- views.fetch(viewId, projectRef)
      _  = assertNotDeprecated(f)
    } yield ()
  }

  test("Undeprecate rejects when view doesn't exist") {
    val doesntExist = nxv + "doesntexist"
    views.undeprecate(doesntExist, projectRef, 1).interceptEquals(ViewNotFound(doesntExist, projectRef))
  }

  test("Undeprecate rejects when incorrect revision is provided") {
    val viewId = genString()
    for {
      _ <- views.create(viewId, projectRef, indexingValue)
      _ <- views.deprecate(viewId, projectRef, 1)
      _ <- views.undeprecate(viewId, projectRef, 42).interceptEquals(IncorrectRev(42, 2))
      r <- views.fetch(viewId, projectRef)
      _  = assertDeprecated(r)
    } yield ()
  }

  test("Undeprecate rejects when view is not deprecated") {
    val viewId = genString()
    for {
      _ <- views.create(viewId, projectRef, indexingValue)
      _ <- views.undeprecate(viewId, projectRef, 1).interceptEquals(ViewIsNotDeprecated(nxv + viewId))
    } yield ()
  }

  // Fetching a view

  test("Fetch a view") {
    val expected = resourceFor(
      indexingViewId,
      projectRef,
      updatedIndexingValue,
      uuid,
      updatedIndexingSource,
      2,
      indexingRev = 2,
      createdBy = bob,
      updatedBy = bob
    )
    views.fetch(indexingViewId, projectRef).assertEquals(expected)
  }

  test("Fetch a view by rev") {
    val expected =
      resourceFor(indexingViewId, projectRef, indexingValue, uuid, indexingSource, createdBy = bob, updatedBy = bob)
    views.fetch(IdSegmentRef(indexingViewId, 1), projectRef).assertEquals(expected)
  }

  test("Fetch rejects when fetching a view by tag") {
    val id = IdSegmentRef.Tag(aggregateViewId, tag)
    views.fetch(id, projectRef).interceptEquals(FetchByTagNotSupported(id))
  }

  test("Fetch rejects when the revision does not exist") {
    views.fetch(IdSegmentRef(indexingViewId, 42), projectRef).interceptEquals(RevisionNotFound(42, 2))
  }

  test("Fetch rejects when the view is not found") {
    views.fetch(doesntExistId, projectRef).interceptEquals(ViewNotFound(doesntExistId, projectRef))
  }

  // Writing to the default view should fail

  test("Deprecating the default view is rejected") {
    val defaultViewId = nxv + "defaultSparqlIndex"
    views.deprecate(defaultViewId, projectRef, 1).interceptEquals(ViewIsDefaultView)
  }

  test("Updating the default view is rejected") {
    val defaultViewId = nxv + "defaultSparqlIndex"
    views.update(defaultViewId, projectRef, 1, indexingSource).interceptEquals(ViewIsDefaultView)
  }
}
