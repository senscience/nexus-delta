package ai.senscience.nexus.delta.plugins.compositeviews.deletion

import ai.senscience.nexus.delta.plugins.compositeviews.CompositeViewsFixture
import ai.senscience.nexus.delta.plugins.compositeviews.indexing.CompositeViewDef
import ai.senscience.nexus.delta.plugins.compositeviews.indexing.CompositeViewDef.{ActiveViewDef, DeprecatedViewDef}
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sdk.views.ViewRef
import ai.senscience.nexus.delta.sourcing.model.Identity.{Anonymous, Subject}
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.effect.{IO, Ref}
import fs2.Stream

import java.util.UUID

class CompositeViewsDeletionTaskSuite extends NexusSuite with CompositeViewsFixture {

  private given Subject = Anonymous

  private val rev = 2

  private val active1    = ViewRef(projectRef, nxv + "active1")
  private val active2    = ViewRef(projectRef, nxv + "active2")
  private val deprecated = ViewRef(projectRef, nxv + "deprecated")

  private def activeView(ref: ViewRef) = ActiveViewDef(
    ref,
    UUID.randomUUID(),
    rev,
    viewValue
  )

  private val viewStream: Stream[IO, CompositeViewDef] =
    Stream(
      activeView(active1),
      DeprecatedViewDef(deprecated),
      activeView(active2)
    )

  test("Deprecate all active views for project") {
    for {
      deprecated   <- Ref.of[IO, Set[ViewRef]](Set.empty)
      deprecateView = (view: ActiveViewDef) => deprecated.getAndUpdate(_ + view.ref).void
      deletionTask  = new CompositeViewsDeletionTask(_ => viewStream, (view, _) => deprecateView(view))
      result       <- deletionTask(projectRef)
      _             = assertEquals(result.log.size, 2, s"'$active1' and '$active2' should appear in the result:\n$result")
      _             = deprecated.get.assertEquals(Set(active1, active2))
    } yield ()
  }

}
