package ai.senscience.nexus.delta.elasticsearch.indexing

import ai.senscience.nexus.delta.elasticsearch.client.IndexLabel
import ai.senscience.nexus.delta.elasticsearch.indexing.ElasticSearchRunningStore.ElasticRunningView
import ai.senscience.nexus.delta.elasticsearch.indexing.IndexingViewDef.ActiveViewDef
import ai.senscience.nexus.delta.elasticsearch.model.ElasticsearchIndexDef
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sdk.views.{IndexingRev, ViewRef}
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.query.SelectFilter
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.effect.{IO, Ref}

import java.util.UUID

class ElasticProjectionLifecycleSuite extends NexusSuite {

  private val project     = ProjectRef.unsafe("org", "proj")
  private val ref         = ViewRef(project, nxv + "view1")
  private val indexingRev = IndexingRev.init
  private val uuid        = UUID.randomUUID()
  private val index       = IndexLabel.unsafe("view1")
  private val view        =
    ActiveViewDef(ref, None, SelectFilter.latest, index, ElasticsearchIndexDef.empty, None, indexingRev, 1, uuid)
  private val running     = ElasticRunningView(ref, indexingRev, uuid)

  // Builds a lifecycle recording the create/delete calls in-memory, backed by an in-memory running store.
  private def setup =
    for {
      created  <- Ref.of[IO, Set[IndexLabel]](Set.empty)
      deleted  <- Ref.of[IO, Set[ElasticRunningView]](Set.empty)
      store     = ElasticSearchRunningStore.inMemory
      lifecycle = ElasticProjectionLifecycle(
                    _ => IO.raiseError(new IllegalStateException("compile is not exercised here")),
                    v => created.update(_ + v.index),
                    v => deleted.update(_ + v),
                    store
                  )
    } yield (lifecycle, created, deleted)

  test("init creates the index and records the running view, destroy deletes it and forgets it") {
    for {
      (lifecycle, created, deleted) <- setup
      _                             <- lifecycle.init(view)
      _                             <- created.get.assertEquals(Set(index))
      _                             <- lifecycle.recorded(ref).assertEquals(List(running))
      _                             <- lifecycle.destroy(running)
      _                             <- deleted.get.assertEquals(Set(running))
      _                             <- lifecycle.recorded(ref).assertEquals(List.empty)
    } yield ()
  }

}
