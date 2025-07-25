package ai.senscience.nexus.delta.elasticsearch

import ai.senscience.nexus.delta.elasticsearch.model.ElasticSearchViewValue.IndexingElasticSearchViewValue
import ai.senscience.nexus.delta.elasticsearch.model.ElasticSearchViewValue.IndexingElasticSearchViewValue.nextIndexingRev
import ai.senscience.nexus.delta.elasticsearch.model.permissions
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue.ContextObject
import ai.senscience.nexus.delta.sdk.views.{IndexingRev, PipeStep}
import ai.senscience.nexus.delta.sourcing.model.Tag.UserTag
import ai.senscience.nexus.delta.sourcing.stream.pipes.FilterDeprecated
import io.circe.JsonObject
import munit.FunSuite

class ElasticSearchViewValueSuite extends FunSuite {

  private val viewValue = IndexingElasticSearchViewValue(None, None)

  test("Views with non-reindexing differences") {
    val viewValues = List(
      IndexingElasticSearchViewValue(Some("name"), None),
      IndexingElasticSearchViewValue(None, Some("description")),
      IndexingElasticSearchViewValue(Some("name"), Some("description")),
      viewValue.copy(permission = permissions.read)
    )
    val expected   = IndexingRev.init
    viewValues.foreach(v => assertEquals(nextIndexingRev(v, viewValue, IndexingRev.init, 2), expected))
  }

  test("Views with different reindexing fields") {
    val viewValues = List(
      viewValue.copy(resourceTag = Some(UserTag.unsafe("tag"))),
      viewValue.copy(pipeline = List(PipeStep(FilterDeprecated.ref.label, None, None))),
      viewValue.copy(mapping = Some(JsonObject.empty)),
      viewValue.copy(settings = Some(JsonObject.empty)),
      viewValue.copy(context = Some(ContextObject.apply(JsonObject.empty)))
    )
    val expected   = IndexingRev(2)
    viewValues.foreach(v => assertEquals(nextIndexingRev(v, viewValue, IndexingRev.init, 2), expected))
  }

}
