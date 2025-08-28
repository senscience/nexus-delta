package ai.senscience.nexus.delta.plugins.blazegraph.supervision

import ai.senscience.nexus.delta.plugins.blazegraph.indexing.CurrentActiveViews
import ai.senscience.nexus.delta.plugins.blazegraph.indexing.IndexingViewDef.ActiveViewDef
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sdk.views.ViewRef
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.query.SelectFilter
import ai.senscience.nexus.testkit.mu.NexusSuite

class BlazegraphViewByNamespaceSuite extends NexusSuite {

  test("Get the different views by their namespace value") {
    val indexingRev = 1
    val rev         = 2
    val project     = ProjectRef.unsafe("org", "proj")

    def activeView(suffix: String) = {
      val id = nxv + suffix
      ActiveViewDef(
        ViewRef(project, id),
        projection = id.toString,
        SelectFilter.latest,
        None,
        namespace = suffix,
        indexingRev,
        rev
      )
    }

    val view1 = activeView("view1")
    val view2 = activeView("view2")

    val currentViews = CurrentActiveViews(view1, view2)

    val expected = Map(view1.namespace -> view1.ref, view2.namespace -> view2.ref)
    BlazegraphViewByNamespace(currentViews).get.assertEquals(expected)
  }

}
