package ai.senscience.nexus.delta.plugins.blazegraph.model

import ai.senscience.nexus.delta.plugins.blazegraph.model.BlazegraphViewValue.{nextIndexingRev, IndexingBlazegraphViewValue}
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sourcing.model.IriFilter
import ai.senscience.nexus.delta.sourcing.model.Tag.UserTag
import munit.FunSuite

class BlazegraphViewValueSuite extends FunSuite {

  private val viewValue = IndexingBlazegraphViewValue(None, None)

  test("Views with non-reindexing differences") {
    val viewValues = List(
      IndexingBlazegraphViewValue(Some("name"), None),
      IndexingBlazegraphViewValue(None, Some("description")),
      IndexingBlazegraphViewValue(Some("name"), Some("description")),
      viewValue.copy(permission = permissions.read)
    )
    viewValues.foreach(v => assertEquals(nextIndexingRev(v, viewValue, 1), 1))
  }

  test("Views with different reindexing fields") {
    val viewValues = List(
      viewValue.copy(resourceSchemas = IriFilter.restrictedTo(Iri.unsafe("http://localhost/schema"))),
      viewValue.copy(resourceTypes = IriFilter.restrictedTo(Iri.unsafe("https://localhost/type"))),
      viewValue.copy(resourceTag = Some(UserTag.unsafe("tag"))),
      viewValue.copy(includeMetadata = true),
      viewValue.copy(includeDeprecated = true)
    )
    viewValues.foreach(v => assertEquals(nextIndexingRev(v, viewValue, 1), 2))
  }

}
