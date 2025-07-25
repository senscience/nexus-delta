package ai.senscience.nexus.delta.elasticsearch

import ai.senscience.nexus.delta.elasticsearch.ElasticSearchViewsQuerySuite.Sample
import ai.senscience.nexus.delta.elasticsearch.client.ElasticSearchAction
import ai.senscience.nexus.delta.elasticsearch.model.ElasticSearchViewRejection.{DifferentElasticSearchViewType, ViewIsDeprecated, ViewNotFound}
import ai.senscience.nexus.delta.elasticsearch.model.ElasticSearchViewValue.{AggregateElasticSearchViewValue, IndexingElasticSearchViewValue}
import ai.senscience.nexus.delta.elasticsearch.model.{permissions, ElasticSearchViewType}
import ai.senscience.nexus.delta.elasticsearch.views.DefaultIndexDef
import ai.senscience.nexus.delta.kernel.utils.UUIDF
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.rdf.jsonld.api.JsonLdApi
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.sdk.acls.AclSimpleCheck
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress
import ai.senscience.nexus.delta.sdk.error.ServiceError.AuthorizationFailed
import ai.senscience.nexus.delta.sdk.generators.{ProjectGen, ResourceGen}
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.permissions.model.Permission
import ai.senscience.nexus.delta.sdk.projects.FetchContextDummy
import ai.senscience.nexus.delta.sdk.resolvers.ResolverContextResolution
import ai.senscience.nexus.delta.sdk.views.{PipeStep, ViewRef}
import ai.senscience.nexus.delta.sdk.{ConfigFixtures, DataResource}
import ai.senscience.nexus.delta.sourcing.model.Identity.{Anonymous, Group, Subject, User}
import ai.senscience.nexus.delta.sourcing.model.{Label, ResourceRef}
import ai.senscience.nexus.delta.sourcing.postgres.Doobie
import ai.senscience.nexus.delta.sourcing.stream.pipes.{DiscardMetadata, FilterDeprecated}
import cats.data.NonEmptySet
import cats.effect.IO
import cats.syntax.all.*
import io.circe.{Decoder, Json, JsonObject}
import munit.{AnyFixture, Location}
import org.http4s.Query

import java.time.Instant
import scala.concurrent.duration.*

class ElasticSearchViewsQuerySuite
    extends NexusElasticsearchSuite
    with Doobie.Fixture
    with ElasticSearchClientSetup.Fixture
    with Fixtures
    with ConfigFixtures {

  override def munitFixtures: Seq[AnyFixture[?]] = List(esClient, doobie)

  implicit private val baseUri: BaseUri = BaseUri.unsafe("http://localhost", "v1")
  implicit private val uuidF: UUIDF     = UUIDF.random

  private val prefix = "prefix"

  private lazy val client = esClient()
  private lazy val xas    = doobie()

  private val realm           = Label.unsafe("myrealm")
  private val alice: Caller   = Caller(User("Alice", realm), Set(User("Alice", realm), Group("users", realm)))
  private val bob: Caller     = Caller(User("Bob", realm), Set(User("Bob", realm), Group("users", realm)))
  private val charlie: Caller = Caller(User("Charlie", realm), Set(User("Charlie", realm), Group("users", realm)))
  private val anon: Caller    = Caller(Anonymous, Set(Anonymous))

  private val project1 = ProjectGen.project("org", "proj")
  private val project2 = ProjectGen.project("org2", "proj2")

  private val queryPermission = Permission.unsafe("views/query")

  private val aclCheck = AclSimpleCheck(
    // Bob has full access
    (bob.subject, AclAddress.Root, Set(queryPermission, permissions.read)),
    // Alice has access to views in project 1
    (alice.subject, AclAddress.Project(project1.ref), Set(queryPermission, permissions.read, permissions.write)),
    // Charlie has access to views in project 2
    (charlie.subject, AclAddress.Project(project2.ref), Set(queryPermission, permissions.read))
  ).accepted

  private val defaultIndexDef = DefaultIndexDef(loader).unsafeRunSync()

  private val indexingValue: IndexingElasticSearchViewValue =
    IndexingElasticSearchViewValue(
      resourceTag = None,
      pipeline = List(PipeStep.noConfig(FilterDeprecated.ref), PipeStep.noConfig(DiscardMetadata.ref)),
      mapping = Some(defaultIndexDef.mapping),
      settings = None,
      permission = queryPermission,
      context = None
    )

  // Indexing views for project 1
  private val view1Proj1 = ViewRef(project1.ref, nxv + "view1Proj1")
  private val view2Proj1 = ViewRef(project1.ref, nxv + "view2Proj1")

  // Indexing views for project 2
  private val view1Proj2 = ViewRef(project2.ref, nxv + "view1Proj2")
  private val view2Proj2 = ViewRef(project2.ref, nxv + "view2Proj2")

  // Aggregates all views of project1
  private val aggregate1      = ViewRef(project1.ref, nxv + "aggregate1")
  private val aggregate1Views = AggregateElasticSearchViewValue(
    Some("AggregateView1"),
    Some("Aggregate of views from project1"),
    NonEmptySet.of(view1Proj1, view2Proj1)
  )

  // Aggregates:
  // * view1Proj2
  // * references an aggregated view on project 2
  // * references the previous aggregate which aggregates all views of project1
  private val aggregate2      = ViewRef(project2.ref, nxv + "aggregate2")
  private val aggregate2Views = AggregateElasticSearchViewValue(
    Some("AggregateView2"),
    Some("Aggregate view1proj2 and aggregate of project1"),
    NonEmptySet.of(view1Proj2, aggregate1)
  )

  // Aggregates:
  // * view2 of project2
  private val aggregate3      = ViewRef(project1.ref, nxv + "aggregate3")
  private val aggregate3Views = AggregateElasticSearchViewValue(
    Some("AggregateView3"),
    Some("Aggregate view2proj2 and aggregate2"),
    NonEmptySet.of(view2Proj2, aggregate2)
  )

  private val allIndexingViews: List[ViewRef] = List(view1Proj1, view2Proj1, view1Proj2, view2Proj2)

  // Resources are indexed in every view
  private def epochPlus(plus: Long) = Instant.EPOCH.plusSeconds(plus)
  private val orgType               = nxv + "Organization"
  private val orgSchema             = ResourceRef.Latest(nxv + "org")
  private val bbp                   =
    Sample(
      "bbp",
      Set(orgType),
      2,
      deprecated = false,
      orgSchema,
      createdAt = epochPlus(5L),
      updatedAt = epochPlus(10L),
      createdBy = alice.subject
    )
  private val epfl                  =
    Sample(
      "epfl",
      Set(orgType),
      1,
      deprecated = false,
      orgSchema,
      createdAt = epochPlus(10L),
      updatedAt = epochPlus(10L),
      updatedBy = alice.subject
    )
  private val datasetSchema         = ResourceRef.Latest(nxv + "dataset")
  private val traceTypes            = Set(nxv + "Dataset", nxv + "Trace")
  private val trace                 = Sample(
    "trace",
    traceTypes,
    3,
    deprecated = false,
    datasetSchema,
    createdAt = epochPlus(15L),
    updatedAt = epochPlus(30L)
  )
  private val cellTypes             = Set(nxv + "Dataset", nxv + "Cell")
  private val cell                  =
    Sample(
      "cell",
      cellTypes,
      3,
      deprecated = true,
      datasetSchema,
      createdAt = epochPlus(20L),
      updatedAt = epochPlus(40L),
      createdBy = alice.subject
    )

  private val allResources = List(bbp, epfl, trace, cell)

  private val fetchContext = FetchContextDummy(List(project1, project2))

  private lazy val views = ElasticSearchViews(
    fetchContext,
    ResolverContextResolution(rcr),
    ValidateElasticSearchView(
      _ => Right(()),
      IO.pure(Set(queryPermission)),
      client.createIndex(_, _, _).void,
      prefix,
      10,
      xas,
      defaultIndexDef
    ),
    eventLogConfig,
    prefix,
    xas,
    defaultIndexDef,
    clock
  ).unsafeRunSync()

  private lazy val viewsQuery = ElasticSearchViewsQuery(
    aclCheck,
    views,
    client,
    prefix,
    xas
  )

  object Ids {

    /**
      * Extract ids from documents from an Elasticsearch search raw response
      */
    def extractAll(json: Json)(implicit loc: Location): Seq[Iri] = {
      for {
        hits    <- json.hcursor.downField("hits").get[Vector[Json]]("hits")
        sources <- hits.traverse(_.hcursor.get[Json]("_source"))
        ids      = extract(sources)
      } yield ids
    }.rightValue

    def extract(results: Seq[Json])(implicit loc: Location): Seq[Iri] =
      results.traverse(extract).rightValue

    def extract(json: Json): Decoder.Result[Iri] = json.hcursor.get[Iri]("@id")

  }

  private val noParameters = Query.empty

  // Match all resources and sort them by created date and date
  private val matchAllSorted                               = jobj"""{ "size": 100, "sort": [{ "_createdAt": "asc" }, { "@id": "asc" }] }"""
  //  private val sort                                         = SortList.byCreationDateAndId
  implicit private val defaultSort: Ordering[DataResource] = Ordering.by { r => r.createdAt -> r.id }

  /**
    * Generate ids for the provided samples for the given view and sort them by creation date and id
    */
  private def generateIds(view: ViewRef, resources: List[Sample]): Seq[Iri] =
    resources.map(_.asResourceF(view)).sorted.map(_.id)

  /**
    * Generate ids for the provided samples for the given views and sort them by creation date and id
    */
  private def generateIds(views: List[ViewRef], resources: List[Sample]): Seq[Iri] =
    views.flatMap { view => resources.map(_.asResourceF(view)) }.sorted.map(_.id)

  test("Init views and populate indices") {
    implicit val caller: Caller         = alice
    val createIndexingViews             = allIndexingViews.traverse { viewRef =>
      views.create(viewRef.viewId, viewRef.project, indexingValue)
    }
    val populateIndexingViews: IO[Unit] = allIndexingViews.traverse { ref =>
      for {
        view <- views.fetchIndexingView(ref.viewId, ref.project)
        bulk <- allResources.traverse { r =>
                  r.asDocument(ref).map { d =>
                    // We create a unique id across all indices
                    ElasticSearchAction.Index(view.index, genString(), None, d)
                  }
                }
        _    <- client.bulk(bulk)
        // We refresh explicitly
        _    <- client.refresh(view.index)
      } yield ()
    }.void

    val createAggregateViews = for {
      _ <- views.create(aggregate1.viewId, aggregate1.project, aggregate1Views)
      _ <- views.create(aggregate2.viewId, aggregate2.project, aggregate2Views)
      _ <- views.create(aggregate3.viewId, aggregate3.project, aggregate3Views)
    } yield ()

    // Create cycle to make sure this case is correctly handled
    val createCycle = {
      val targetedViews = NonEmptySet.of(view1Proj1, view2Proj1, aggregate3)
      val newValue      = AggregateElasticSearchViewValue(targetedViews)
      views.update(aggregate1.viewId, aggregate1.project, 1, newValue)
    }

    (createIndexingViews >> populateIndexingViews >> createAggregateViews >> createCycle).void
      .assertEquals(())
      .unsafeRunSync()
  }

  test("Query for all documents in a view") {
    implicit val caller: Caller = alice
    val expectedIds             = generateIds(view1Proj1, allResources)
    viewsQuery
      .query(view1Proj1, matchAllSorted, noParameters)
      .map(Ids.extractAll)
      .assertEquals(expectedIds)
  }

  test("Query a view without permissions") {
    implicit val caller: Caller = anon
    viewsQuery.query(view1Proj1, JsonObject.empty, Query.empty).intercept[AuthorizationFailed]
  }

  test("Query the deprecated view should raise an deprecation error") {
    implicit val caller: Caller = alice
    val deprecated              = ViewRef(project1.ref, nxv + "deprecated")
    for {
      _ <- views.create(deprecated.viewId, deprecated.project, indexingValue)
      _ <- views.deprecate(deprecated.viewId, deprecated.project, 1)
      _ <- viewsQuery
             .query(deprecated, matchAllSorted, noParameters)
             .interceptEquals(ViewIsDeprecated(deprecated.viewId))
    } yield ()
  }

  test("Query an aggregate view with a user with full access") {
    implicit val caller: Caller = bob
    val accessibleViews         = List(view1Proj1, view2Proj1, view1Proj2, view2Proj2)
    val expectedIds             = generateIds(accessibleViews, allResources)
    viewsQuery
      .query(aggregate2, matchAllSorted, noParameters)
      .map(Ids.extractAll)
      .assertEquals(expectedIds)
  }

  test("Query an aggregate view with a user with limited access") {
    implicit val caller: Caller = alice
    val accessibleViews         = List(view1Proj1, view2Proj1)
    val expectedIds             = generateIds(accessibleViews, allResources)
    viewsQuery
      .query(aggregate2, matchAllSorted, noParameters)
      .map(Ids.extractAll)
      .assertEquals(expectedIds)
  }

  test("Query an aggregate view with a user with no access") {
    implicit val caller: Caller = anon
    val expectedIds             = List.empty
    viewsQuery
      .query(aggregate2, matchAllSorted, noParameters)
      .map(Ids.extractAll)
      .assertEquals(expectedIds)
  }

  test("Obtaining the mapping without permission should fail") {
    implicit val caller: Caller = anon
    viewsQuery
      .mapping(view1Proj1.viewId, project1.ref)
      .intercept[AuthorizationFailed]
  }

  test("Obtaining the mapping for a view that doesn't exist in the project should fail") {
    implicit val caller: Caller = alice
    viewsQuery
      .mapping(view1Proj2.viewId, project1.ref)
      .interceptEquals(ViewNotFound(view1Proj2.viewId, project1.ref))
  }

  test("Obtaining the mapping on an aggregate view should fail") {
    implicit val caller: Caller = alice
    viewsQuery
      .mapping(aggregate1.viewId, project1.ref)
      .interceptEquals(
        DifferentElasticSearchViewType(
          aggregate1.viewId.toString,
          ElasticSearchViewType.AggregateElasticSearch,
          ElasticSearchViewType.ElasticSearch
        )
      )

  }

  test("Obtaining the mapping with views/write permission should succeed") {
    implicit val caller: Caller = alice
    viewsQuery.mapping(view1Proj1.viewId, project1.ref)
  }

  test("Creating a point in time without permission should fail") {
    implicit val caller: Caller = anon
    viewsQuery
      .createPointInTime(view1Proj1.viewId, project1.ref, 30.seconds)
      .intercept[AuthorizationFailed]
  }

  test("Creating a point in time for a view that doesn't exist in the project should fail") {
    implicit val caller: Caller = alice
    viewsQuery
      .createPointInTime(view1Proj2.viewId, project1.ref, 30.seconds)
      .interceptEquals(ViewNotFound(view1Proj2.viewId, project1.ref))
  }

  test("Creating and deleting a point in time with the right access should succeed") {
    implicit val caller: Caller = alice
    viewsQuery
      .createPointInTime(view1Proj1.viewId, project1.ref, 30.seconds)
      .flatMap { pit =>
        viewsQuery.deletePointInTime(pit)
      }
  }
}

object ElasticSearchViewsQuerySuite {

  final private case class Sample(
      suffix: String,
      types: Set[Iri],
      rev: Int,
      deprecated: Boolean,
      schema: ResourceRef,
      createdAt: Instant,
      updatedAt: Instant,
      createdBy: Subject = Anonymous,
      updatedBy: Subject = Anonymous
  ) {

    def asResourceF(view: ViewRef)(implicit rcr: RemoteContextResolution): DataResource = {
      val resource = ResourceGen.resource(view.viewId / suffix, view.project, Json.obj())
      ResourceGen
        .resourceFor(resource, types = types, rev = rev, deprecated = deprecated)
        .copy(
          createdAt = createdAt,
          createdBy = createdBy,
          updatedAt = updatedAt,
          updatedBy = updatedBy,
          schema = schema
        )
    }

    def asDocument(
        view: ViewRef
    )(implicit baseUri: BaseUri, rcr: RemoteContextResolution, jsonldApi: JsonLdApi): IO[Json] =
      asResourceF(view).toCompactedJsonLd.map(_.json)

  }
}
