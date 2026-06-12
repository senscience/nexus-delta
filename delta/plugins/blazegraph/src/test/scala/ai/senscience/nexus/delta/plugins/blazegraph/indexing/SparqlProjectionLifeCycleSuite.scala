package ai.senscience.nexus.delta.plugins.blazegraph.indexing

import ai.senscience.nexus.delta.plugins.blazegraph.indexing.IndexingViewDef.ActiveViewDef
import ai.senscience.nexus.delta.plugins.blazegraph.indexing.SparqlRunningStore.SparqlRunningView
import ai.senscience.nexus.delta.plugins.blazegraph.model.defaultViewId
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sdk.views.ViewRef
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.query.SelectFilter
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.effect.{IO, Ref}

import java.util.UUID

class SparqlProjectionLifeCycleSuite extends NexusSuite {

  private val project     = ProjectRef.unsafe("org", "proj")
  private val indexingRev = 1
  private val rev         = 2
  private val uuid        = UUID.randomUUID()

  private def activeView(ref: ViewRef): ActiveViewDef =
    ActiveViewDef(ref, SelectFilter.latest, None, s"namespace-${ref.viewId}", indexingRev, rev, uuid)

  private val view        = activeView(ViewRef(project, nxv + "view1"))
  private val running     = SparqlRunningView(view.ref, view.indexingRev, view.uuid)
  private val defaultView = activeView(ViewRef(project, defaultViewId))

  // Builds a lifecycle recording the namespace create/delete calls in-memory, backed by an in-memory running store.
  private def setup =
    for {
      created  <- Ref.of[IO, Set[String]](Set.empty)
      deleted  <- Ref.of[IO, Set[SparqlRunningView]](Set.empty)
      store     = SparqlRunningStore.inMemory
      lifecycle = SparqlProjectionLifeCycle(
                    _ => IO.raiseError(new IllegalStateException("compile is not exercised here")),
                    v => created.update(_ + v.namespace),
                    v => deleted.update(_ + v),
                    store
                  )
    } yield (lifecycle, created, deleted, store)

  test("init creates the namespace and records the running view, destroy deletes it and forgets it") {
    for {
      (lifecycle, created, deleted, _) <- setup
      _                                <- lifecycle.init(view)
      _                                <- created.get.assertEquals(Set(view.namespace))
      _                                <- lifecycle.recorded(view.ref).assertEquals(List(running))
      _                                <- lifecycle.destroy(running)
      _                                <- deleted.get.assertEquals(Set(running))
      _                                <- lifecycle.recorded(view.ref).assertEquals(List.empty)
    } yield ()
  }

  test("init creates the default view namespace but never records it") {
    for {
      (lifecycle, created, _, store) <- setup
      _                              <- lifecycle.init(defaultView)
      _                              <- created.get.assertEquals(Set(defaultView.namespace))
      _                              <- store.list(defaultView.ref).assertEquals(List.empty)
    } yield ()
  }

}
