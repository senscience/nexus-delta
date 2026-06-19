package ai.senscience.nexus.delta.plugins.blazegraph.indexing

import ai.senscience.nexus.delta.plugins.blazegraph.BlazegraphViews
import ai.senscience.nexus.delta.plugins.blazegraph.indexing.SparqlRunningStore.SparqlRunningView
import ai.senscience.nexus.delta.sdk.views.ViewRef
import ai.senscience.nexus.delta.sourcing.Transactors
import ai.senscience.nexus.delta.sourcing.implicits.given
import ai.senscience.nexus.delta.sourcing.stream.ProjectionMetadata
import cats.effect.{IO, Ref}
import org.typelevel.doobie.*
import org.typelevel.doobie.postgres.implicits.*
import org.typelevel.doobie.syntax.all.*

import java.util.UUID

/**
  * Durable record of the projection materialized by the coordinator for each Blazegraph view revision.
  */
trait SparqlRunningStore {

  /**
    * Records the given view revision.
    */
  def save(view: SparqlRunningView): IO[Unit]

  /**
    * All recorded revisions for the given view.
    */
  def list(ref: ViewRef): IO[List[SparqlRunningView]]

  /**
    * Removes the record for the given view revision.
    */
  def delete(ref: ViewRef, indexingRev: Int): IO[Unit]
}

object SparqlRunningStore {

  final case class SparqlRunningView(ref: ViewRef, indexingRev: Int, uuid: UUID) {

    val projection: String = BlazegraphViews.projectionName(ref.project, ref.viewId, indexingRev)

    def metadata: ProjectionMetadata =
      ProjectionMetadata(BlazegraphViews.entityType.value, projection, Some(ref.project), Some(ref.viewId))
  }

  def apply(xas: Transactors): SparqlRunningStore = new SparqlRunningStore {

    override def save(view: SparqlRunningView): IO[Unit] =
      sql"""INSERT INTO public.blazegraph_running_views (project, view_id, indexing_rev, uuid, instant)
           |VALUES (${view.ref.project}, ${view.ref.viewId}, ${view.indexingRev}, ${view.uuid}, NOW())
           |ON CONFLICT (project, view_id, indexing_rev) DO UPDATE SET instant = NOW()
           |""".stripMargin.update.run.transact(xas.write).void

    override def list(ref: ViewRef): IO[List[SparqlRunningView]] =
      sql"""SELECT indexing_rev, uuid FROM public.blazegraph_running_views
           |WHERE project = ${ref.project} AND view_id = ${ref.viewId}
           |""".stripMargin
        .query[(Int, UUID)]
        .to[List]
        .transact(xas.read)
        .map(_.map { case (indexingRev, uuid) => SparqlRunningView(ref, indexingRev, uuid) })

    override def delete(ref: ViewRef, indexingRev: Int): IO[Unit] =
      sql"""DELETE FROM public.blazegraph_running_views
           |WHERE project = ${ref.project} AND view_id = ${ref.viewId} AND indexing_rev = $indexingRev
           |""".stripMargin.update.run.transact(xas.write).void
  }

  /**
    * An in-memory store, for tests.
    */
  def inMemory: SparqlRunningStore = {
    val ref = Ref.unsafe[IO, Map[(ViewRef, Int), UUID]](Map.empty)
    new SparqlRunningStore {
      override def save(view: SparqlRunningView): IO[Unit] =
        ref.update(_.updated((view.ref, view.indexingRev), view.uuid))

      override def list(viewRef: ViewRef): IO[List[SparqlRunningView]] =
        ref.get.map(_.collect {
          case ((r, indexingRev), uuid) if r == viewRef => SparqlRunningView(r, indexingRev, uuid)
        }.toList)

      override def delete(viewRef: ViewRef, indexingRev: Int): IO[Unit] =
        ref.update(_.removed((viewRef, indexingRev)))
    }
  }
}
