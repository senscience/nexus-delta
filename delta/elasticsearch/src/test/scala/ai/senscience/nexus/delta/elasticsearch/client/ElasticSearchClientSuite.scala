package ai.senscience.nexus.delta.elasticsearch.client

import ai.senscience.nexus.delta.elasticsearch.client.BulkResponse.MixedOutcomes.Outcome
import ai.senscience.nexus.delta.elasticsearch.client.Refresh.WaitFor
import ai.senscience.nexus.delta.elasticsearch.model.ElasticsearchIndexDef
import ai.senscience.nexus.delta.elasticsearch.query.ElasticSearchClientError.{ElasticsearchCreateIndexError, ElasticsearchQueryError, ElasticsearchUpdateMappingError}
import ai.senscience.nexus.delta.elasticsearch.{ElasticSearchClientSetup, NexusElasticsearchSuite}
import ai.senscience.nexus.delta.kernel.dependency.ComponentDescription.ServiceDescription
import ai.senscience.nexus.delta.kernel.search.Pagination.FromPagination
import ai.senscience.nexus.delta.sdk.model.search.ResultEntry.ScoredResultEntry
import ai.senscience.nexus.delta.sdk.model.search.SearchResults.ScoredSearchResults
import ai.senscience.nexus.delta.sdk.model.search.{SearchResults, Sort, SortList}
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.testkit.CirceLiteral
import ai.senscience.nexus.testkit.elasticsearch.ElasticSearchContainer
import ai.senscience.nexus.testkit.mu.ce.PatienceConfig
import cats.effect.IO
import io.circe.syntax.EncoderOps
import io.circe.{Json, JsonObject}
import munit.AnyFixture

import scala.concurrent.duration.*

class ElasticSearchClientSuite extends NexusElasticsearchSuite with ElasticSearchClientSetup.Fixture with CirceLiteral {

  private given PatienceConfig = PatienceConfig(5.seconds, 10.millis)

  private val page = FromPagination(0, 100)

  override def munitFixtures: Seq[AnyFixture[?]] = List(esClient)

  private lazy val client = esClient()

  private def searchAllIn(index: IndexLabel): IO[Seq[JsonObject]] =
    client.search(QueryBuilder.empty.withPage(page), Set(index.value)).map(_.sources)

  private def replaceAndRefresh(index: IndexLabel, id: String, document: JsonObject) =
    client.replace(index, id, document) >> client.refresh(index)

  private def generateIndexLabel = IndexLabel.unsafe(genString())

  test("Fetch the service description") {
    client.serviceDescription.assertEquals(ServiceDescription("elasticsearch", ElasticSearchContainer.Version))
  }

  test("Verify that an index does not exist then create it") {
    val index = generateIndexLabel
    for {
      _ <- client.existsIndex(index).assertEquals(false)
      _ <- client.createIndex(index, ElasticsearchIndexDef.empty).assertEquals(true)
      _ <- client.existsIndex(index).assertEquals(true)
      _ <- client.createIndex(index, ElasticsearchIndexDef.empty).assertEquals(false)
    } yield ()
  }

  test("Fail to create an index with wrong payload") {
    val index    = generateIndexLabel
    val wrongDef = ElasticsearchIndexDef.fromJson(jobj"""{"key": "value"}""", None)
    client.createIndex(index, wrongDef).intercept[ElasticsearchCreateIndexError]
  }

  test("Delete an index") {
    val index = generateIndexLabel
    for {
      indexingDef <- ElasticsearchIndexDef.fromClasspath(
                       "defaults/default-mapping.json",
                       Some("defaults/default-settings.json"),
                       "number_of_shards" -> 1
                     )
      _           <- client.createIndex(index, indexingDef).assertEquals(true)
      _           <- client.deleteIndex(index).assertEquals(true)
      _           <- client.deleteIndex(index).assertEquals(false)
    } yield ()
  }

  test("Create an index and updates its mapping") {
    val index = generateIndexLabel

    val originalMapping = jobj"""{ "properties": { "city": { "type": "text" } } }"""
    val indexingViewDef = ElasticsearchIndexDef.fromJson(originalMapping, None)
    val updatedMapping  = ElasticsearchMappings(
      jobj"""{ "properties": { "street": { "type": "text" },  "city": { "type": "text" }} }"""
    )

    val expected =
      jobj"""{ "mappings": { "properties": { "street": { "type": "text" },  "city": { "type": "text" }} } }"""

    def downToMapping(json: Json) = json.hcursor.downField(index.value).focus

    client.createIndex(index, indexingViewDef).assertEquals(true) >>
      client.updateMapping(index, updatedMapping) >>
      client
        .mapping(index)
        .map(downToMapping)
        .assertEquals(Some(expected.asJson))
  }

  test("Fail to update mapping when it introduces a incompatible change") {
    val index = generateIndexLabel

    val originalMapping = jobj"""{ "properties": { "city": { "type": "text" } } }"""
    val indexingViewDef = ElasticsearchIndexDef.fromJson(originalMapping, None)
    val updatedMapping  = ElasticsearchMappings(jobj"""{ "properties": { "city": { "type": "integer" }} }""")

    client.createIndex(index, indexingViewDef).assertEquals(true) >>
      client.updateMapping(index, updatedMapping).intercept[ElasticsearchUpdateMappingError]
  }

  test("Create an index and updates its settings") {
    val index = generateIndexLabel

    val originalMapping = jobj"""{ "properties": { "city": { "type": "text" } } }"""
    val indexingViewDef = ElasticsearchIndexDef.fromJson(originalMapping, None)
    val newSettings     = ElasticsearchSettings(
      jobj"""{ "index": { "refresh_interval" : "-1" } }"""
    )

    def downToRefreshInterval(json: Json) =
      json.hcursor.downField(index.value).downField("settings").downField("index").get[Int]("refresh_interval").toOption

    client.createIndex(index, indexingViewDef).assertEquals(true) >>
      client.updateSettings(index, newSettings) >>
      client
        .settings(index)
        .map(downToRefreshInterval)
        .assertEquals(Some(-1))
  }

  test("Attempt to delete a non-existing index") {
    client.deleteIndex(generateIndexLabel).assertEquals(false)
  }

  test("Replace a document") {
    val index           = generateIndexLabel
    val document        = jobj"""{"key": "value"}"""
    val documentUpdated = jobj"""{"key": "value2"}"""
    for {
      _ <- client.createIndex(index, ElasticsearchIndexDef.empty)
      _ <- replaceAndRefresh(index, "1", document)
      _ <- searchAllIn(index).assertEquals(Vector(document))
      _ <- replaceAndRefresh(index, "1", documentUpdated)
      _ <- searchAllIn(index).assertEquals(Vector(documentUpdated))
    } yield ()
  }

  test("Run bulk operations") {
    val index      = generateIndexLabel
    val operations = List(
      ElasticSearchAction.Index(index, "1", Some("routing"), json"""{ "field1" : "value1" }"""),
      ElasticSearchAction.Delete(index, "2", Some("routing2")),
      ElasticSearchAction.Index(index, "2", Some("routing2"), json"""{ "field1" : "value1" }"""),
      ElasticSearchAction.Delete(index, "2", Some("routing2")),
      ElasticSearchAction.Create(index, "3", Some("routing"), json"""{ "field1" : "value3" }"""),
      ElasticSearchAction.Update(index, "1", Some("routing"), json"""{ "doc" : {"field2" : "value2"} }""")
    )

    val expectedDocuments = Vector(jobj"""{"field1": "value3"}""", jobj"""{"field1": "value1", "field2" : "value2"}""")

    client.bulk(operations, WaitFor).assertEquals(BulkResponse.Success) >>
      searchAllIn(index).assertEquals(expectedDocuments)
  }

  test("Run bulk operations with errors") {
    val index      = generateIndexLabel
    val operations = List(
      ElasticSearchAction.Index(index, "1", None, json"""{ "field1" : "value1" }"""),
      ElasticSearchAction.Delete(index, "2", None),
      ElasticSearchAction.Index(index, "2", None, json"""{ "field1" : 27 }"""),
      ElasticSearchAction.Delete(index, "3", None),
      ElasticSearchAction.Create(index, "3", None, json"""{ "field1" : "value3" }"""),
      ElasticSearchAction.Update(index, "5", None, json"""{ "doc" : {"field2" : "value2"} }""")
    )

    client.bulk(operations).map {
      case BulkResponse.Success              => fail("errors expected")
      case BulkResponse.MixedOutcomes(items) =>
        val (failures, successes) = items.partitionMap { case (key, outcome) =>
          Either.cond(outcome == Outcome.Success(key), key, key)
        }
        assertEquals(successes, List("1", "2", "3"))
        assertEquals(failures, List("5"))
    }
  }

  test("Get the source of the given document") {
    val index = generateIndexLabel
    val doc   = jobj"""{ "field1" : 1 }"""

    replaceAndRefresh(index, "1", doc) >>
      client.getSource[Json](index, "1").assertEquals(Some(doc.asJson)) >>
      client.getSource[Json](index, "x").assertEquals(None)
  }

  test("Count") {
    val index      = generateIndexLabel
    val operations = List(
      ElasticSearchAction.Index(index, "1", None, json"""{ "field1" : 1 }"""),
      ElasticSearchAction.Index(index, "2", None, json"""{ "field1" : 2 }"""),
      ElasticSearchAction.Index(index, "3", None, json"""{ "doc" : {"field2" : 4} }""")
    )
    client.bulk(operations, Refresh.WaitFor) >>
      client.count(index.value).assertEquals(3L)
  }

  test("Search") {
    val index      = generateIndexLabel
    val operations = List(
      ElasticSearchAction.Index(index, "1", None, json"""{ "field1" : 1 }"""),
      ElasticSearchAction.Create(index, "3", None, json"""{ "field1" : 3 }"""),
      ElasticSearchAction.Update(index, "1", None, json"""{ "doc" : {"field2" : "value2"} }""")
    )

    for {
      _        <- client.bulk(operations, Refresh.WaitFor)
      query     = QueryBuilder
                    .unsafe(jobj"""{"query": {"bool": {"must": {"exists": {"field": "field1"} } } } }""")
                    .withPage(page)
                    .withSort(SortList(List(Sort("-field1"))))
      expected  = SearchResults(2, Vector(jobj"""{ "field1" : 3 }""", jobj"""{ "field1" : 1, "field2" : "value2"}"""))
                    .copy(token = Some("[1]"))
      _        <- client.search(query, Set(index.value)).assertEquals(expected)
      query2    = QueryBuilder.unsafe(jobj"""{"query": {"bool": {"must": {"term": {"field1": 3} } } } }""").withPage(page)
      expected2 = ScoredSearchResults(1, 1f, Vector(ScoredResultEntry(1f, jobj"""{ "field1" : 3 }""")))
      _        <- client.search(query2, Set(index.value)).assertEquals(expected2)
    } yield ()
  }

  test("Search returning raw results") {
    val index      = generateIndexLabel
    val operations = List(
      ElasticSearchAction.Index(index, "1", None, json"""{ "field1" : 1 }"""),
      ElasticSearchAction.Create(index, "3", None, json"""{ "field1" : 3 }"""),
      ElasticSearchAction.Update(index, "1", None, json"""{ "doc" : {"field2" : "value2"} }""")
    )

    for {
      _               <- client.bulk(operations, Refresh.WaitFor)
      request          = ElasticSearchRequest(jobj"""{"query": {"bool": {"must": {"term": {"field1": 3} } } } }""")
      expectedResults <- loader.jsonContentOf("elasticsearch-results.json", "index" -> index)
      _               <- client
                           .search(request, Set(index.value))
                           .map(_.removeKeys("took"))
                           .assertEquals(expectedResults)
    } yield ()
  }

  test("Fail for an invalid search") {
    val index   = generateIndexLabel
    val request = ElasticSearchRequest(jobj"""{ "query": { "other": {} } }""")
    client.search(request, Set(index.value)).intercept[ElasticsearchQueryError]
  }

  test("Delete documents by query") {
    val index      = generateIndexLabel
    val operations = List(
      ElasticSearchAction.Index(index, "1", None, json"""{ "field1" : 1 }"""),
      ElasticSearchAction.Create(index, "2", None, json"""{ "field1" : 3 }""")
    )

    for {
      // Indexing and checking count
      _      <- client.bulk(operations, Refresh.WaitFor)
      _      <- client.count(index.value).assertEquals(2L)
      // Deleting document matching the given query
      request = ElasticSearchRequest(jobj"""{"query": {"bool": {"must": {"term": {"field1": 3} } } } }""")
      _      <- client.deleteByQuery(request, index)
      _      <- client.count(index.value).assertEquals(1L).eventually
      _      <- client.getSource[Json](index, "1")
      _      <- client.getSource[Json](index, "2").assertEquals(None)
    } yield ()
  }

  test("Create a point in time for the given index") {
    val index = generateIndexLabel
    for {
      _   <- client.createIndex(index, ElasticsearchIndexDef.empty)
      pit <- client.createPointInTime(index, 30.seconds)
      _   <- client.deletePointInTime(pit)
    } yield ()
  }

}
