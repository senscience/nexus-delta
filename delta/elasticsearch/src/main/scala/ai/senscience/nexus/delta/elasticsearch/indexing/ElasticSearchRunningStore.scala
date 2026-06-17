package ai.senscience.nexus.delta.elasticsearch.indexing

import ai.senscience.nexus.delta.elasticsearch.ElasticSearchViews
import ai.senscience.nexus.delta.elasticsearch.indexing.ElasticSearchRunningStore.ElasticRunningView
import ai.senscience.nexus.delta.sdk.views.{IndexingRev, ViewRef}
import ai.senscience.nexus.delta.sourcing.Transactors
import ai.senscience.nexus.delta.sourcing.implicits.given
import ai.senscience.nexus.delta.sourcing.stream.ProjectionMetadata
import cats.effect.{IO, Ref}
import doobie.*
import doobie.postgres.implicits.*
import doobie.syntax.all.*

import java.util.UUID

/**
  * Durable record of the projection materialized by the coordinator for each Elasticsearch view revision.
  */
trait ElasticSearchRunningStore {

  /**
    * Records the given view revision.
    */
  def save(view: ElasticRunningView): IO[Unit]

  /**
    * All recorded revisions for the given view.
    */
  def list(ref: ViewRef): IO[List[ElasticRunningView]]

  /**
    * Removes the record for the given view revision.
    */
  def delete(ref: ViewRef, indexingRev: IndexingRev): IO[Unit]
}

object ElasticSearchRunningStore {

  final case class ElasticRunningView(ref: ViewRef, indexingRev: IndexingRev, uuid: UUID) {

    val projection: String = ElasticSearchViews.projectionName(ref.project, ref.viewId, indexingRev)

    def metadata: ProjectionMetadata =
      ProjectionMetadata(ElasticSearchViews.entityType.value, projection, Some(ref.project), Some(ref.viewId))
  }

  def apply(xas: Transactors): ElasticSearchRunningStore = new ElasticSearchRunningStore {

    override def save(view: ElasticRunningView): IO[Unit] =
      sql"""INSERT INTO public.elasticsearch_running_views (project, view_id, indexing_rev, uuid, instant)
           |VALUES (${view.ref.project}, ${view.ref.viewId}, ${view.indexingRev}, ${view.uuid}, NOW())
           |ON CONFLICT (project, view_id, indexing_rev) DO UPDATE SET instant = NOW()
           |""".stripMargin.update.run.transact(xas.write).void

    override def list(ref: ViewRef): IO[List[ElasticRunningView]] =
      sql"""SELECT indexing_rev, uuid FROM public.elasticsearch_running_views
           |WHERE project = ${ref.project} AND view_id = ${ref.viewId}
           |""".stripMargin
        .query[(IndexingRev, UUID)]
        .to[List]
        .transact(xas.read)
        .map(_.map { case (indexingRev, uuid) => ElasticRunningView(ref, indexingRev, uuid) })

    override def delete(ref: ViewRef, indexingRev: IndexingRev): IO[Unit] =
      sql"""DELETE FROM public.elasticsearch_running_views
           |WHERE project = ${ref.project} AND view_id = ${ref.viewId} AND indexing_rev = $indexingRev
           |""".stripMargin.update.run.transact(xas.write).void
  }

  /**
    * An in-memory store, for tests.
    */
  def inMemory: ElasticSearchRunningStore = {
    val ref = Ref.unsafe[IO, Map[(ViewRef, IndexingRev), UUID]](Map.empty)
    new ElasticSearchRunningStore {
      override def save(view: ElasticRunningView): IO[Unit] =
        ref.update(_.updated((view.ref, view.indexingRev), view.uuid))

      override def list(viewRef: ViewRef): IO[List[ElasticRunningView]] =
        ref.get.map(_.collect {
          case ((r, indexingRev), uuid) if r == viewRef => ElasticRunningView(r, indexingRev, uuid)
        }.toList)

      override def delete(viewRef: ViewRef, indexingRev: IndexingRev): IO[Unit] =
        ref.update(_.removed((viewRef, indexingRev)))
    }
  }
}
