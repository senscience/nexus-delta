package ai.senscience.nexus.delta.plugins.blazegraph

import ai.senscience.nexus.delta.plugins.blazegraph.indexing.CurrentActiveViews
import ai.senscience.nexus.delta.plugins.blazegraph.indexing.IndexingViewDef.ActiveViewDef
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sdk.views.ViewRef
import ai.senscience.nexus.delta.sourcing.model.Identity.{Anonymous, Subject}
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.query.SelectFilter
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.effect.{IO, Ref}

class BlazegraphDeletionTaskSuite extends NexusSuite {

  private given Subject = Anonymous

  private val project     = ProjectRef.unsafe("org", "proj")
  private val indexingRev = 1
  private val rev         = 2

  private val active1 = ViewRef(project, nxv + "active1")
  private val active2 = ViewRef(project, nxv + "active2")

  private def activeView(ref: ViewRef) = ActiveViewDef(
    ref,
    projection = ref.viewId.toString,
    SelectFilter.latest,
    None,
    namespace = "view2",
    indexingRev,
    rev
  )

  private val currentViews = CurrentActiveViews(activeView(active1), activeView(active2))

  test("Deprecate all active views for project") {
    for {
      deprecated   <- Ref.of[IO, Set[ViewRef]](Set.empty)
      deprecateView = (view: ActiveViewDef) => deprecated.getAndUpdate(_ + view.ref).void
      deletionTask  = new BlazegraphDeletionTask(currentViews, (view, _) => deprecateView(view))
      result       <- deletionTask(project)
      _             = assertEquals(result.log.size, 2, s"'$active1' and '$active2' should appear in the result:\n$result")
      _             = deprecated.get.assertEquals(Set(active1, active2))
    } yield ()
  }

}
