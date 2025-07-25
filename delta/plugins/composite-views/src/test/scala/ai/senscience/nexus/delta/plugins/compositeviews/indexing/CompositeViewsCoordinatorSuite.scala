package ai.senscience.nexus.delta.plugins.compositeviews.indexing

import ai.senscience.nexus.delta.kernel.cache.LocalCache
import ai.senscience.nexus.delta.plugins.compositeviews.CompositeViewsFixture
import ai.senscience.nexus.delta.plugins.compositeviews.indexing.CompositeViewDef.ActiveViewDef
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sdk.views.ViewRef
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.effect.{IO, Ref}
import cats.syntax.all.*

import java.util.UUID

class CompositeViewsCoordinatorSuite extends NexusSuite with CompositeViewsFixture {

  private val existingViewRef                    = ViewRef(projectRef, nxv + "my-view")
  private val anotherView                        = ViewRef(projectRef, nxv + "another-view")
  private def activeView(ref: ViewRef, rev: Int) = ActiveViewDef(
    ref,
    UUID.randomUUID(),
    rev,
    viewValue
  )

  // Apply clean up and return the affected view if any
  private def cleanup(newView: CompositeViewDef, cachedViews: List[ActiveViewDef]) = {
    for {
      // Creates the cache
      cache        <- LocalCache[ViewRef, ActiveViewDef]().flatTap { c =>
                        cachedViews.traverse { v => c.put(v.ref, v) }
                      }
      // The destroy action
      destroyed    <- Ref.of[IO, Option[ActiveViewDef]](None)
      destroy       = (active: ActiveViewDef, _: CompositeViewDef) =>
                        destroyed.getAndUpdate {
                          case Some(current) =>
                            throw new IllegalArgumentException(s"Destroy has already been called on $current")
                          case None          => Some(active)
                        }.void
      _            <- CompositeViewsCoordinator.cleanupCurrent(cache, newView, destroy)
      destroyedOpt <- destroyed.get
    } yield destroyedOpt

  }

  test("Do not trigger clean up if the the view is not running yet") {
    val existingView = activeView(existingViewRef, 1)
    cleanup(existingView, List.empty).assertEquals(None)
  }

  test("Trigger clean up if the view is running") {
    val existingView = activeView(existingViewRef, 1)
    cleanup(existingView, List(existingView)).assertEquals(Some(existingView))
  }

  test("Do not trigger clean up for a new view") {
    val existingView = activeView(existingViewRef, 1)
    val another      = activeView(anotherView, 3)
    cleanup(another, List(existingView)).assertEquals(None)
  }

}
