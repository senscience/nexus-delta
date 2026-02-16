package ai.senscience.nexus.delta.elasticsearch

import ai.senscience.nexus.delta.elasticsearch.client.*
import ai.senscience.nexus.delta.elasticsearch.model.*
import ai.senscience.nexus.delta.elasticsearch.model.ElasticSearchViewRejection.*
import ai.senscience.nexus.delta.elasticsearch.model.ElasticSearchViewValue.{AggregateElasticSearchViewValue, IndexingElasticSearchViewValue}
import ai.senscience.nexus.delta.elasticsearch.model.permissions.query as queryPermissions
import ai.senscience.nexus.delta.elasticsearch.views.DefaultIndexDef
import ai.senscience.nexus.delta.kernel.utils.UUIDF
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.{nxv, schema}
import ai.senscience.nexus.delta.sdk.ConfigFixtures
import ai.senscience.nexus.delta.sdk.generators.ProjectGen
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.implicits.{*, given}
import ai.senscience.nexus.delta.sdk.model.*
import ai.senscience.nexus.delta.sdk.projects.FetchContextDummy
import ai.senscience.nexus.delta.sdk.projects.model.ProjectRejection.{ProjectIsDeprecated, ProjectNotFound}
import ai.senscience.nexus.delta.sdk.projects.model.ApiMappings
import ai.senscience.nexus.delta.sdk.resolvers.ResolverContextResolution
import ai.senscience.nexus.delta.sdk.views.{IndexingRev, PipeStep, ViewRef}
import ai.senscience.nexus.delta.sourcing.EntityDependencyStore
import ai.senscience.nexus.delta.sourcing.model.EntityDependency.DependsOn
import ai.senscience.nexus.delta.sourcing.model.Identity.{Group, Subject, User}
import ai.senscience.nexus.delta.sourcing.model.Tag.UserTag
import ai.senscience.nexus.delta.sourcing.model.{IriFilter, Label, ProjectRef, Tags}
import ai.senscience.nexus.delta.sourcing.postgres.Doobie
import ai.senscience.nexus.delta.sourcing.stream.pipes.{FilterBySchema, FilterByType, FilterDeprecated}
import ai.senscience.nexus.testkit.mu.NexusSuite
import munit.AnyFixture
import cats.data.NonEmptySet
import cats.effect.IO
import io.circe.{Json, JsonObject}
import io.circe.syntax.{EncoderOps, KeyOps}

import java.time.Instant
import java.util.UUID

class ElasticSearchViewsSuite extends NexusSuite with Doobie.Fixture with ConfigFixtures with Fixtures {

  override def munitFixtures: Seq[AnyFixture[?]] = List(doobie)

  private lazy val xas = doobie()

  private val realm                   = Label.unsafe("myrealm")
  private given aliceSubject: Subject = User("Alice", realm)
  private given alice: Caller         = Caller(aliceSubject, Set(User("Alice", realm), Group("users", realm)))

  private val uuid    = UUID.randomUUID()
  private given UUIDF = UUIDF.fixed(uuid)

  private val org         = Label.unsafe("org")
  private val apiMappings = ApiMappings("nxv" -> nxv.base, "Person" -> schema.Person)
  private val base        = nxv.base
  private val project     = ProjectGen.project("org", "proj", base = base, mappings = apiMappings)
  private val listProject = ProjectGen.project("org", "list", base = base, mappings = apiMappings)

  private val projectRef           = ProjectRef.unsafe("org", "proj")
  private val deprecatedProjectRef = ProjectRef.unsafe("org", "proj-deprecated")
  private val unknownProjectRef    = ProjectRef(org, Label.unsafe("xxx"))

  private val defaultIndexDef = DefaultIndexDef.fromJson(JsonObject.empty, JsonObject.empty)

  private val indexDef = ElasticsearchIndexDef.fromJson(
    JsonObject("dynamic" := false),
    Some(JsonObject("analysis" := JsonObject.empty))
  )
  private val mapping  = indexDef.mappings
  private val settings = indexDef.settings.get

  private val viewId          = iri"http://localhost/indexing"
  private val aggregateViewId = iri"http://localhost/${genString()}"
  private val viewPipelineId  = iri"http://localhost/indexing-pipeline"
  private val viewId2         = iri"http://localhost/${genString()}"

  private lazy val fetchContext = FetchContextDummy(
    Map(project.ref -> project.context, listProject.ref -> listProject.context),
    Set(deprecatedProjectRef)
  )

  private lazy val views: ElasticSearchViews = ElasticSearchViews(
    fetchContext,
    ResolverContextResolution(rcr),
    ValidateElasticSearchView(
      pipeChainCompiler,
      IO.pure(Set(queryPermissions)),
      (_, _) => IO.unit,
      "prefix",
      2,
      xas,
      defaultIndexDef
    ),
    eventLogConfig,
    "prefix",
    xas,
    defaultIndexDef,
    clock
  ).accepted

  private def resourceFor(
      id: Iri,
      value: ElasticSearchViewValue,
      source: Json,
      rev: Int = 1,
      indexingRev: IndexingRev = IndexingRev.init,
      deprecated: Boolean = false
  ): ViewResource =
    ElasticSearchViewState(
      id = id,
      project = project.ref,
      uuid = uuid,
      value = value,
      source = source,
      tags = Tags.empty,
      rev = rev,
      indexingRev = indexingRev,
      deprecated = deprecated,
      createdAt = Instant.EPOCH,
      createdBy = alice.subject,
      updatedAt = Instant.EPOCH,
      updatedBy = alice.subject
    ).toResource(defaultIndexDef)

  private def givenAView[A](test: String => IO[A]): IO[A] = {
    val id     = genString()
    val source = json"""{"@type": "ElasticSearchView", "mapping": ${mapping.asJson}}"""
    views.create(id, projectRef, source) >> test(id)
  }

  private def givenADeprecatedView[A](test: String => IO[A]): IO[A] =
    givenAView { view =>
      views.deprecate(view, projectRef, 1) >> test(view)
    }

  // Creating views

  test("Create a view with minimum fields for an IndexingElasticSearchViewValue") {
    val source = json"""{"@type": "ElasticSearchView", "mapping": ${mapping.asJson}}"""
    views.create(projectRef, source).void
  }

  test("Create a view with a fixed id specified in the IndexingElasticSearchViewValue json") {
    val source   =
      json"""{"@id": "$viewId", "@type": "ElasticSearchView", "mapping": ${mapping.asJson}, "settings": ${settings.asJson}}"""
    val expected = resourceFor(
      id = viewId,
      value = IndexingElasticSearchViewValue(
        resourceTag = None,
        IndexingElasticSearchViewValue.defaultPipeline,
        mapping = Some(mapping),
        settings = Some(settings),
        context = None,
        permission = queryPermissions
      ),
      source = source
    )
    views.create(projectRef, source).assertEquals(expected)
  }

  test("Create a view with a valid pipeline") {
    val source =
      json"""{"@id": "$viewPipelineId", "@type": "ElasticSearchView", "pipeline": [{"name": "filterDeprecated"}], "mapping": ${mapping.asJson} }"""
    views.create(projectRef, source).void
  }

  test("Create a view using an IndexingElasticSearchViewValue") {
    val value = IndexingElasticSearchViewValue(
      resourceTag = Some(UserTag.unsafe("tag")),
      List(
        PipeStep(FilterBySchema(IriFilter.restrictedTo(iri"http://localhost/schema"))),
        PipeStep(FilterByType(IriFilter.restrictedTo(iri"http://localhost/type"))),
        PipeStep.noConfig(FilterDeprecated.ref)
      ),
      mapping = Some(mapping),
      settings = None,
      context = None,
      permission = queryPermissions
    )
    views.create(viewId2, projectRef, value).void
  }

  test("Create a view with a fixed id specified in the AggregateElasticSearchViewValue json") {
    val id     = iri"http://localhost/${genString()}"
    val source =
      json"""
        {"@type": "AggregateElasticSearchView",
         "@id": "$id",
         "views": [
           {
             "project": "${project.ref.toString}",
             "viewId": "$viewId"
           }
         ]}"""
    views.create(projectRef, source).void
  }

  test("Create a view using an AggregateElasticSearchViewValue") {
    val value = AggregateElasticSearchViewValue(NonEmptySet.of(ViewRef(projectRef, viewId)))
    for {
      _ <- views.create(aggregateViewId, projectRef, value)
      d <- EntityDependencyStore.directDependencies(projectRef, aggregateViewId, xas)
      _  = assertEquals(d, Set(DependsOn(projectRef, viewId)))
    } yield ()
  }

  // Rejecting view creation

  test("Reject creating a view when it already exists") {
    val source = json"""{"@type": "ElasticSearchView", "mapping": ${mapping.asJson}}"""
    for {
      _ <- views.create(projectRef, source).intercept[ResourceAlreadyExists]
      _ <- views.create(viewId, projectRef, source).intercept[ResourceAlreadyExists]
    } yield ()
  }

  test("Reject creating a view when the pipe does not exist") {
    val id     = iri"http://localhost/${genString()}"
    val source =
      json"""{"@type": "ElasticSearchView", "pipeline": [ { "name": "xxx" } ], "mapping": ${mapping.asJson} }"""
    views.create(id, projectRef, source).intercept[InvalidPipeline]
  }

  test("Reject creating a view when the pipe configuration is invalid") {
    val id     = iri"http://localhost/${genString()}"
    val source =
      json"""{"@type": "ElasticSearchView", "pipeline": [ { "name": "filterByType" } ], "mapping": ${mapping.asJson} }"""
    views.create(id, projectRef, source).intercept[InvalidPipeline]
  }

  test("Reject creating a view when the permission is not defined") {
    val id     = iri"http://localhost/${genString()}"
    val source = json"""{"@type": "ElasticSearchView", "mapping": ${mapping.asJson}, "permission": "not/exists"}"""
    views.create(id, projectRef, source).intercept[PermissionIsNotDefined]
  }

  test("Reject creating a view when the referenced view does not exist") {
    val id           = iri"http://localhost/${genString()}"
    val referencedId = iri"http://localhost/${genString()}"
    val value        = AggregateElasticSearchViewValue(NonEmptySet.of(ViewRef(projectRef, referencedId)))
    views.create(id, projectRef, value).intercept[InvalidViewReferences]
  }

  test("Reject creating a view when too many views are referenced") {
    val id    = iri"http://localhost/${genString()}"
    val value = AggregateElasticSearchViewValue(
      NonEmptySet.of(ViewRef(projectRef, aggregateViewId), ViewRef(projectRef, viewPipelineId))
    )
    views.create(id, projectRef, value).intercept[TooManyViewReferences]
  }

  test("Reject creating a view when the referenced project does not exist") {
    val id    = iri"http://localhost/${genString()}"
    val value = AggregateElasticSearchViewValue(NonEmptySet.of(ViewRef(unknownProjectRef, viewId)))
    views.create(id, projectRef, value).intercept[InvalidViewReferences]
  }

  test("Reject creating a view when the referenced project is deprecated") {
    val id     = iri"http://localhost/${genString()}"
    val source = json"""{"@type": "ElasticSearchView", "mapping": ${mapping.asJson}}"""
    views.create(id, deprecatedProjectRef, source).intercept[ProjectIsDeprecated]
  }

  // Updating views

  test("Update a view with minimum fields for an IndexingElasticSearchViewValue") {
    val source = json"""{"@type": "ElasticSearchView", "mapping": ${mapping.asJson}}"""
    views.update(viewId, projectRef, 1, source).void
  }

  test("Update a view with a fixed id specified in the AggregateElasticSearchViewValue json") {
    val id     = iri"http://localhost/${genString()}"
    val source =
      json"""
        {"@type": "AggregateElasticSearchView",
         "@id": "$id",
         "views": [
           {
             "project": "${project.ref.toString}",
             "viewId": "$viewId"
           }
         ]}"""
    views.create(projectRef, source) >> views.update(id, projectRef, 1, source).void
  }

  // Rejecting view update

  test("Reject updating a view with incorrect revision for IndexingElasticSearchViewValue") {
    val source = json"""{"@type": "ElasticSearchView", "mapping": ${mapping.asJson}}"""
    views.update(viewId, projectRef, 100, source).intercept[IncorrectRev]
  }

  test("Reject updating a view with incorrect revision for AggregateElasticSearchViewValue") {
    val id     = iri"http://localhost/${genString()}"
    val source =
      json"""
        {"@type": "AggregateElasticSearchView",
         "@id": "$id",
         "views": [
           {
             "project": "${project.ref.toString}",
             "viewId": "$viewId"
           }
         ]}"""
    views.create(projectRef, source) >> views.update(id, projectRef, 100, source).intercept[IncorrectRev]
  }

  test("Reject updating an IndexingElasticSearchViewValue with an AggregateElasticSearchViewValue") {
    val source =
      json"""
        {"@type": "AggregateElasticSearchView",
         "views": [
           {
             "project": "${project.ref.toString}",
             "viewId": "$viewId"
           }
         ]}"""
    views.update(viewId, projectRef, 2, source).intercept[DifferentElasticSearchViewType]
  }

  test("Reject updating a deprecated view") {
    val id     = iri"http://localhost/${genString()}"
    val source = json"""{"@type": "ElasticSearchView", "mapping": ${mapping.asJson}}"""
    for {
      _ <- views.create(id, projectRef, source)
      _ <- views.deprecate(id, projectRef, 1)
      _ <- views.update(id, projectRef, 2, source).intercept[ViewIsDeprecated]
    } yield ()
  }

  test("Reject updating a view that is not found") {
    val id     = iri"http://localhost/${genString()}"
    val source = json"""{"@type": "ElasticSearchView", "mapping": ${mapping.asJson}}"""
    views.update(id, projectRef, 1, source).intercept[ViewNotFound]
  }

  test("Reject updating a view when the project is not found") {
    val id     = iri"http://localhost/${genString()}"
    val source = json"""{"@type": "ElasticSearchView", "mapping": ${mapping.asJson}}"""
    views.update(id, unknownProjectRef, 1, source).intercept[ProjectNotFound]
  }

  test("Reject updating a view when the project is deprecated") {
    val id     = iri"http://localhost/${genString()}"
    val source = json"""{"@type": "ElasticSearchView", "mapping": ${mapping.asJson}}"""
    views.update(id, deprecatedProjectRef, 1, source).intercept[ProjectIsDeprecated]
  }

  // Deprecating views

  test("Deprecate a view with the correct revision") {
    views.deprecate(viewId, projectRef, 2).void
  }

  // Rejecting view deprecation

  test("Reject deprecating an already deprecated view") {
    views.deprecate(viewId, projectRef, 3).intercept[ViewIsDeprecated]
  }

  test("Reject deprecating a view with incorrect revision") {
    views.deprecate(viewId, projectRef, 100).intercept[IncorrectRev]
  }

  test("Reject deprecating a view that is not found") {
    val id = iri"http://localhost/${genString()}"
    views.deprecate(id, projectRef, 1).intercept[ViewNotFound]
  }

  test("Reject deprecating a view when the project is not found") {
    val id = iri"http://localhost/${genString()}"
    views.deprecate(id, unknownProjectRef, 1).intercept[ProjectNotFound]
  }

  test("Reject deprecating a view when the project is deprecated") {
    val id = iri"http://localhost/${genString()}"
    views.deprecate(id, deprecatedProjectRef, 1).intercept[ProjectIsDeprecated]
  }

  // Undeprecating views

  test("Undeprecate a view with the correct revision") {
    givenADeprecatedView { view =>
      for {
        result  <- views.undeprecate(view, projectRef, 2)
        _        = assert(!result.deprecated)
        fetched <- views.fetch(view, projectRef)
        _        = assert(!fetched.deprecated)
      } yield ()
    }
  }

  // Rejecting view undeprecation

  test("Reject undeprecating a view that is not deprecated") {
    givenAView { view =>
      views.undeprecate(view, projectRef, 1).intercept[ViewIsNotDeprecated]
    }
  }

  test("Reject undeprecating a view with incorrect revision") {
    givenADeprecatedView { view =>
      views.undeprecate(view, projectRef, 100).intercept[IncorrectRev]
    }
  }

  test("Reject undeprecating a view that is not found") {
    val nonExistentView = iri"http://localhost/${genString()}"
    views.undeprecate(nonExistentView, projectRef, 2).intercept[ViewNotFound]
  }

  test("Reject undeprecating a view when the project is not found") {
    givenAView { view =>
      views.undeprecate(view, unknownProjectRef, 2).intercept[ProjectNotFound]
    }
  }

  test("Reject undeprecating a view when the project is deprecated") {
    val id = iri"http://localhost/${genString()}"
    views.undeprecate(id, deprecatedProjectRef, 2).intercept[ProjectIsDeprecated]
  }

  // Fetching views

  test("Fetch a view by id") {
    val id       = iri"http://localhost/${genString()}"
    val source   = json"""{"@type": "ElasticSearchView", "mapping": ${mapping.asJson}}"""
    val expected = resourceFor(
      id = id,
      value = IndexingElasticSearchViewValue(
        resourceTag = None,
        IndexingElasticSearchViewValue.defaultPipeline,
        mapping = Some(mapping),
        settings = None,
        context = None,
        permission = queryPermissions
      ),
      source = source
    )
    views.create(id, projectRef, source) >> views.fetch(id, projectRef).assertEquals(expected)
  }

  test("Fetch a view by id and rev") {
    val id            = iri"http://localhost/${genString()}"
    val source        = json"""{"@type": "ElasticSearchView", "mapping": ${mapping.asJson}}"""
    val expected      = resourceFor(
      id = id,
      value = IndexingElasticSearchViewValue(
        resourceTag = None,
        IndexingElasticSearchViewValue.defaultPipeline,
        mapping = Some(mapping),
        settings = None,
        context = None,
        permission = queryPermissions
      ),
      source = source
    )
    val updatedSource = json"""{"@type": "ElasticSearchView", "resourceTag": "mytag", "mapping": ${mapping.asJson}}"""
    for {
      _ <- views.create(id, projectRef, source)
      _ <- views.update(id, projectRef, 1, updatedSource)
      _ <- views.fetch(IdSegmentRef(id, 1), projectRef).assertEquals(expected)
    } yield ()
  }

  // Rejecting view fetch

  test("Reject fetching a view that does not exist") {
    val id = iri"http://localhost/${genString()}"
    views.fetch(id, projectRef).intercept[ViewNotFound]
  }

  test("Reject fetching a view at a non-existent revision") {
    val id     = iri"http://localhost/${genString()}"
    val source = json"""{"@type": "ElasticSearchView", "mapping": ${mapping.asJson}}"""
    views.create(id, projectRef, source) >> views.fetch(IdSegmentRef(id, 2), projectRef).intercept[RevisionNotFound]
  }

  test("Reject fetching a view by tag") {
    val tag    = UserTag.unsafe("mytag")
    val id     = iri"http://localhost/${genString()}"
    val source = json"""{"@type": "ElasticSearchView", "mapping": ${mapping.asJson}}"""
    views.create(id, projectRef, source) >>
      views.fetch(IdSegmentRef(id, tag), projectRef).intercept[FetchByTagNotSupported]
  }

  // Writing to the default view should fail

  test("Reject deprecating the default view") {
    val defaultViewId = iri"nxv:defaultElasticSearchIndex"
    views.deprecate(defaultViewId, projectRef, 1).intercept[ViewIsDefaultView]
  }

  test("Reject updating the default view") {
    val defaultViewId = iri"nxv:defaultElasticSearchIndex"
    val source        = json"""{"@type": "ElasticSearchView", "mapping": ${mapping.asJson}}"""
    views.update(defaultViewId, projectRef, 1, source).intercept[ViewIsDefaultView]
  }
}
