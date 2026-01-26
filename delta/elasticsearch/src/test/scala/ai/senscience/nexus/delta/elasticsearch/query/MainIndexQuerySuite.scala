package ai.senscience.nexus.delta.elasticsearch.query

import ai.senscience.nexus.delta.elasticsearch.client.{ElasticSearchAction, ElasticSearchRequest}
import ai.senscience.nexus.delta.elasticsearch.config.MainIndexConfig
import ai.senscience.nexus.delta.elasticsearch.main.MainIndexDef
import ai.senscience.nexus.delta.elasticsearch.model.ResourcesSearchParams
import ai.senscience.nexus.delta.elasticsearch.model.ResourcesSearchParams.Type.{ExcludedType, IncludedType}
import ai.senscience.nexus.delta.elasticsearch.model.ResourcesSearchParams.TypeOperator.{And, Or}
import ai.senscience.nexus.delta.elasticsearch.model.ResourcesSearchParams.{LogParam, TypeOperator, TypeParams, VersionParams}
import ai.senscience.nexus.delta.elasticsearch.query.MainIndexQuerySuite.*
import ai.senscience.nexus.delta.elasticsearch.{ElasticSearchClientSetup, Fixtures, NexusElasticsearchSuite}
import ai.senscience.nexus.delta.kernel.search.Pagination.FromPagination
import ai.senscience.nexus.delta.kernel.search.{Pagination, TimeRange}
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.rdf.jsonld.api.{JsonLdApi, TitaniumJsonLdApi}
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.sdk.DataResource
import ai.senscience.nexus.delta.sdk.generators.ResourceGen
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.model.search.*
import ai.senscience.nexus.delta.sourcing.model.Identity.{Anonymous, Subject, User}
import ai.senscience.nexus.delta.sourcing.model.Tag.UserTag
import ai.senscience.nexus.delta.sourcing.model.{Label, ProjectRef, ResourceRef}
import cats.effect.IO
import cats.syntax.all.*
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Json, JsonObject}
import munit.{AnyFixture, Location}

import java.time.Instant

class MainIndexQuerySuite extends NexusElasticsearchSuite with ElasticSearchClientSetup.Fixture with Fixtures {
  override def munitFixtures: Seq[AnyFixture[?]] = List(esClient)

  private lazy val client = esClient()

  private given JsonLdApi = TitaniumJsonLdApi.strict
  private given BaseUri   = BaseUri.unsafe("http://localhost", "v1")

  private def epochPlus(plus: Long) = Instant.EPOCH.plusSeconds(plus)
  private val realm                 = Label.unsafe("myrealm")
  private val alice                 = User("Alice", realm)
  private val myTag                 = UserTag.unsafe("mytag")

  private val project1    = ProjectRef.unsafe("org", "proj1")
  private val project2    = ProjectRef.unsafe("org", "proj2")
  private val allProjects = Set(project1, project2)

  private val orgType                 = nxv + "Organization"
  private val orgSchema               = ResourceRef.Latest(nxv + "org")
  private val bbp                     =
    Sample(
      "bbp",
      project1,
      Set(orgType),
      2,
      deprecated = false,
      orgSchema,
      createdAt = epochPlus(5L),
      updatedAt = epochPlus(10L),
      createdBy = alice,
      tag = myTag.some
    )
  private val epfl                    =
    Sample(
      "epfl",
      project1,
      Set(orgType),
      1,
      deprecated = false,
      orgSchema,
      createdAt = epochPlus(10L),
      updatedAt = epochPlus(10L),
      updatedBy = alice
    )
  private val datasetSchema           = ResourceRef.Latest(nxv + "dataset")
  private val datasetType: Iri        = nxv + "Dataset"
  private val traceType: Iri          = nxv + "Trace"
  private val traceTypes              = Set(datasetType, traceType)
  private val trace                   = Sample(
    "trace",
    project2,
    traceTypes,
    3,
    deprecated = false,
    datasetSchema,
    createdAt = epochPlus(15L),
    updatedAt = epochPlus(30L)
  )
  private val cellType: Iri           = nxv + "Cell"
  private val cellTypes               = Set(datasetType, cellType)
  private val cell                    =
    Sample(
      "cell",
      project2,
      cellTypes,
      3,
      deprecated = true,
      datasetSchema,
      createdAt = epochPlus(20L),
      updatedAt = epochPlus(40L),
      createdBy = alice
    )
  private val orgs                    = List(bbp, epfl)
  private val deprecated              = List(cell)
  private val createdByAlice          = List(bbp, cell)
  private val createdBetween_8_and_16 = List(epfl, trace)
  private val createdAfter_11         = List(trace, cell)
  private val updatedBefore_12        = List(bbp, epfl)
  private val updatedByAlice          = List(epfl)
  private val allResources            = List(bbp, epfl, trace, cell)

  private val mainIndexConfig     = MainIndexConfig("nexus", "default", 1, 100)
  private val mainIndex           = mainIndexConfig.index
  private lazy val mainIndexQuery = MainIndexQuery(client, mainIndexConfig)

  object Ids {

    /**
      * Extract ids from documents from an Elasticsearch search raw response
      */
    def extractAll(json: Json)(using Location): Seq[Iri] = {
      for {
        hits    <- json.hcursor.downField("hits").get[Vector[Json]]("hits")
        sources <- hits.traverse(_.hcursor.get[Json]("_source"))
        ids      = extract(sources)
      } yield ids
    }.rightValue

    /**
      * Extract ids from documents from results from [[SearchResults]]
      */
    def extractAll(results: SearchResults[JsonObject])(using Location): Seq[Iri] =
      extract(results.sources.map(_.asJson))

    def extract(results: Seq[Json])(using Location): Seq[Iri] =
      results.traverse(extract).rightValue

    def extract(json: Json): Decoder.Result[Iri] = json.hcursor.get[Iri]("@id")
  }

  private def search(params: ResourcesSearchParams, projects: Set[ProjectRef]) =
    paginatedSearch(params, projects, Pagination.FromPagination(0, 100), SortList.byCreationDateAndId)

  private def paginatedSearch(
      params: ResourcesSearchParams,
      projects: Set[ProjectRef],
      pagination: Pagination,
      sort: SortList
  ) =
    mainIndexQuery.list(MainIndexRequest(params, pagination, sort), projects)

  private def aggregate(params: ResourcesSearchParams, projects: Set[ProjectRef]) =
    mainIndexQuery.aggregate(
      MainIndexRequest(params, Pagination.FromPagination(0, 100), SortList.byCreationDateAndId),
      projects
    )

  test("Create the index and populate it ") {
    for {
      mainIndexDef <- MainIndexDef(mainIndexConfig)
      _            <- client.createIndex(mainIndex, mainIndexDef.indexDef)
      bulk         <- allResources.traverse { r =>
                        r.asDocument.map { d =>
                          ElasticSearchAction.Index(mainIndex, genString(), Some(r.project.toString), d)
                        }
                      }
      _            <- client.bulk(bulk)
      // We refresh explicitly
      _            <- client.refresh(mainIndex)
    } yield ()
  }

  private def withTypes(
      included: List[Iri] = List.empty,
      excluded: List[Iri] = List.empty,
      operator: TypeOperator = Or
  ) = ResourcesSearchParams(
    types = TypeParams(
      values = included.map(IncludedType(_)) ++ excluded.map(ExcludedType(_)),
      operator = operator
    )
  )

  private def withCreatedBy(subject: Subject)     = ResourcesSearchParams(log = LogParam(createdBy = Some(subject)))
  private def withUpdatedBy(subject: Subject)     = ResourcesSearchParams(log = LogParam(updatedBy = Some(subject)))
  private def withCreatedAt(timeRange: TimeRange) = ResourcesSearchParams(log = LogParam(createdAt = timeRange))
  private def withUpdatedAt(timeRange: TimeRange) = ResourcesSearchParams(log = LogParam(updatedAt = timeRange))

  private val all                       = ResourcesSearchParams()
  private val orgByType                 = withTypes(included = List(orgType))
  private val datasetAndCellTypes       = withTypes(included = List(datasetType, cellType), operator = And)
  private val datasetOrCellTypes        = withTypes(included = List(datasetType, cellType), operator = Or)
  private val notDatasetAndNotCell      = withTypes(excluded = List(datasetType, cellType), operator = And)
  private val notDatasetOrNotCell       = withTypes(excluded = List(datasetType, cellType), operator = Or)
  private val orgBySchema               = ResourcesSearchParams(schema = Some(orgSchema))
  private val excludeDatasetType        = withTypes(excluded = List(datasetType))
  private val byDeprecated              = ResourcesSearchParams(deprecated = Some(true))
  private val byCreated                 = withCreatedBy(alice)
  private val between_8_and_16          = TimeRange.Between.unsafe(epochPlus(8L), epochPlus(16))
  private val byCreatedBetween_8_and_16 = withCreatedAt(between_8_and_16)
  private val byCreatedAfter_11         = withCreatedAt(TimeRange.After(epochPlus(11L)))
  private val byUpdated                 = withUpdatedBy(alice)
  private val byUpdated_Before_12       = withUpdatedAt(TimeRange.Before(epochPlus(12L)))

  private val bbpResource    = bbp.asResourceF
  private val byId           = ResourcesSearchParams(id = Some(bbpResource.id))
  private val byLocatingId   = ResourcesSearchParams(locate = Some(bbpResource.id))
  private val byLocatingSelf = ResourcesSearchParams(locate = Some(bbpResource.self))
  private val byTag          = ResourcesSearchParams(version = VersionParams(tag = myTag.some))

  // Action / params / matching resources

  List(
    ("all resources", all, allResources),
    ("org resources by type", orgByType, orgs),
    ("types AND", datasetAndCellTypes, List(cell)),
    ("types OR", datasetOrCellTypes, List(trace, cell)),
    ("types exclude AND", notDatasetAndNotCell, List(bbp, epfl)),
    ("types exclude OR", notDatasetOrNotCell, List(bbp, epfl, trace)),
    ("org resources by schema", orgBySchema, orgs),
    ("all resources but the ones with 'Dataset' type", excludeDatasetType, orgs),
    ("deprecated resources", byDeprecated, deprecated),
    ("resources created by Alice", byCreated, createdByAlice),
    ("resources created between 8 and 16", byCreatedBetween_8_and_16, createdBetween_8_and_16),
    ("resources created after 11", byCreatedAfter_11, createdAfter_11),
    ("resources updated by Alice", byUpdated, updatedByAlice),
    ("resources updated before 12", byUpdated_Before_12, updatedBefore_12),
    (s"resources with id ${bbpResource.id}", byId, List(bbp)),
    (s"resources by locating id ${bbpResource.id}", byLocatingId, List(bbp)),
    (s"resources by locating self ${bbpResource.self}", byLocatingSelf, List(bbp)),
    (s"resources with tag ${myTag.value}", byTag, List(bbp))
  ).foreach { case (testName, params, expected) =>
    test(s"Search: $testName") {
      search(params, allProjects).map(Ids.extractAll).assertEquals(expected.map(_.id))
    }
  }

  test("Search on a single project") {
    search(all, Set(project1)).map(Ids.extractAll).assertEquals(orgs.map(_.id))
  }

  test("Apply pagination") {
    val twoPerPage = FromPagination(0, 2)
    val params     = ResourcesSearchParams()

    for {
      results <- paginatedSearch(params, allProjects, twoPerPage, SortList.byCreationDateAndId)
      _        = assertEquals(results.total, 4L)
      _        = assertEquals(results.sources.size, 2)
      // Token from Elasticsearch to fetch the next page
      epflId   = epfl.asResourceF.id
      _        = assertEquals(results.token, Some(s"""[10000,"$epflId"]"""))
    } yield ()
  }

  /** For the given params, executes the aggregation and allows to assert on the result */
  private def assertAggregation(resourcesSearchParams: ResourcesSearchParams)(assertion: AggregationsValue => Unit) =
    aggregate(resourcesSearchParams, allProjects).map { aggregationResult =>
      extractAggs(aggregationResult).map { aggregationValue =>
        assertion(aggregationValue)
      }
    }

  test("Aggregate projects correctly") {
    assertAggregation(all) { agg =>
      assertEquals(agg.projects.buckets.size, 2)
      assert(agg.projects.buckets.contains(Bucket("org/proj1", 2)))
      assert(agg.projects.buckets.contains(Bucket("org/proj2", 2)))
    }
  }

  test("Aggregate types correctly") {
    assertAggregation(all) { agg =>
      assertEquals(agg.types.buckets.size, 4)
      assert(agg.types.buckets.contains(Bucket((nxv + "Organization").toString, 2)))
      assert(agg.types.buckets.contains(Bucket(datasetType.toString, 2)))
      assert(agg.types.buckets.contains(Bucket(cellType.toString, 1)))
      assert(agg.types.buckets.contains(Bucket(traceType.toString, 1)))
    }
  }

  private val matchAllSorted = ElasticSearchRequest(
    jobj"""{ "size": 100, "sort": [{ "_createdAt": "asc" }, { "@id": "asc" }] }"""
  )

  test(s"Search only among $project1") {
    mainIndexQuery
      .search(project1, matchAllSorted)
      .map(Ids.extractAll)
      .assertEquals(orgs.map(_.id))
  }

  test(s"Search only among $project2") {
    mainIndexQuery
      .search(project2, matchAllSorted)
      .map(Ids.extractAll)
      .assertEquals(List(trace.id, cell.id))
  }

  test(s"Search in $project1 to get the trace id returns nothing") {
    val request = ElasticSearchRequest(jobj"""{ "query": { "term": { "@id": "${trace.id}" } } }""")
    mainIndexQuery
      .search(project1, request)
      .map(Ids.extractAll)
      .assertEquals(List.empty)
  }

  test(s"Search in $project2 to get the trace id returns it") {
    val request = ElasticSearchRequest(jobj"""{ "query": { "term": { "@id": "${trace.id}" } } }""")
    mainIndexQuery
      .search(project2, request)
      .map(Ids.extractAll)
      .assertEquals(List(trace.id))
  }

}

object MainIndexQuerySuite {

  final private case class Sample(
      suffix: String,
      project: ProjectRef,
      types: Set[Iri],
      rev: Int,
      deprecated: Boolean,
      schema: ResourceRef,
      createdAt: Instant,
      updatedAt: Instant,
      createdBy: Subject = Anonymous,
      updatedBy: Subject = Anonymous,
      tag: Option[UserTag] = None
  ) {

    def id: Iri = nxv + suffix

    def asResourceF(using RemoteContextResolution): DataResource = {
      val resource = ResourceGen.resource(id, project, Json.obj())
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

    def asDocument(using
        baseUri: BaseUri,
        rcr: RemoteContextResolution,
        jsonldApi: JsonLdApi
    ): IO[Json] = {
      val tags = Json.obj("_tags" -> tag.toList.asJson)
      asResourceF.toCompactedJsonLd.map(_.json.deepMerge(tags))
    }
  }

  case class Bucket(key: String, doc_count: Int)
  case class Aggregation(buckets: List[Bucket])
  case class AggregationsValue(projects: Aggregation, types: Aggregation)

  def extractAggs(aggregation: AggregationResult): Option[AggregationsValue] = {
    import io.circe.generic.auto.*
    aggregation.value.asJson.as[AggregationsValue].toOption
  }

}
