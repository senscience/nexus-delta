package ai.senscience.nexus.delta.plugins.search

import ai.senscience.nexus.delta.elasticsearch.model.ElasticsearchIndexDef
import ai.senscience.nexus.delta.plugins.compositeviews.model.CompositeView
import ai.senscience.nexus.delta.plugins.compositeviews.model.CompositeViewProjectionFields.{ElasticSearchProjectionFields, SparqlProjectionFields}
import ai.senscience.nexus.delta.plugins.search.model.SearchConfig.IndexingConfig
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue.ContextObject
import ai.senscience.nexus.delta.rdf.query.SparqlQuery.SparqlConstructQuery
import ai.senscience.nexus.delta.sdk.Defaults
import ai.senscience.nexus.delta.sourcing.model.IriFilter
import io.circe.{Json, JsonObject}
import munit.FunSuite

import scala.concurrent.duration.*

class SearchViewFactorySuite extends FunSuite {

  private val defaults = Defaults("name", "description")
  private val config   = IndexingConfig(
    resourceTypes = IriFilter.restrictedTo(nxv + "Test"),
    indexDef = ElasticsearchIndexDef.fromJson(
      JsonObject("mapping" -> Json.obj()),
      Some(JsonObject("settings" -> Json.obj()))
    ),
    query = SparqlConstructQuery.unsafe("query"),
    context = ContextObject(JsonObject("context" -> Json.obj())),
    rebuildStrategy = Some(CompositeView.Interval(30.seconds))
  )

  test("Build the search view according to the configuration") {
    val result     = SearchViewFactory(defaults, config)
    assertEquals(result.projections.length, 1)
    val projection = result.projections.head
    projection match {
      case _: SparqlProjectionFields         => fail("Expecting an Elasticsearch projection")
      case es: ElasticSearchProjectionFields =>
        assertEquals(es.query, config.query)
        assertEquals(es.mapping, config.indexDef.mappings)
        assertEquals(es.settings, config.indexDef.settings)
        assertEquals(es.context, config.context)
    }
    assertEquals(result.rebuildStrategy, config.rebuildStrategy)
  }

  test("Build the search view according to the configuration") {
    config
  }

}
