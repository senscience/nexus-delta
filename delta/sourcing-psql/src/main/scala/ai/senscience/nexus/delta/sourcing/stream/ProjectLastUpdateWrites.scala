package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.rdf.Vocabulary
import ai.senscience.nexus.delta.sourcing.Transactors
import ai.senscience.nexus.delta.sourcing.implicits.*
import ai.senscience.nexus.delta.sourcing.model.{EntityType, Label, ProjectRef, Tag}
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.projections.ProjectLastUpdateStore
import ai.senscience.nexus.delta.sourcing.query.{RefreshStrategy, StreamingQuery}
import ai.senscience.nexus.delta.sourcing.stream.Elem.SuccessElem
import ai.senscience.nexus.delta.sourcing.stream.config.BatchConfig
import cats.effect.IO
import doobie.Fragments
import doobie.postgres.implicits.*
import doobie.syntax.all.*

import java.time.Instant

trait ProjectLastUpdateWrites

// $COVERAGE-OFF$
object ProjectLastUpdateWrites {

  private val projectionMetadata: ProjectionMetadata =
    ProjectionMetadata("system", "project-last-updates-writes", None, None)

  // We need a value to return to Distage
  private val dummy = new ProjectLastUpdateWrites {}

  private def whereFilter(offset: Offset) =
    Fragments.whereAndOpt(offset.asFragment, Tag.latest.asFragment)

  private val entityType = EntityType("project-last-update")
  private val iri        = Vocabulary.nxv + "project-last-update"
  private val rev        = -1

  private[stream] def query(offset: Offset, batchSize: Int) =
    sql"""((SELECT org, project, max(instant), max(ordering) as max_ordering
         |FROM public.scoped_states
         |${whereFilter(offset)}
         |GROUP BY org, project
         |ORDER BY max_ordering
         |LIMIT $batchSize)
         |UNION ALL
         |(SELECT org, project, max(instant), max(ordering) as max_ordering
         |FROM public.scoped_tombstones
         |${whereFilter(offset)}
         |GROUP BY org, project
         |ORDER BY max_ordering
         |LIMIT $batchSize)
         |ORDER BY max_ordering)
         |LIMIT $batchSize
         |""".stripMargin.query[(Label, Label, Instant, Offset)].map { case (org, project, lastInstant, lastOrdering) =>
      val projectRef = ProjectRef(org, project)
      SuccessElem(entityType, iri, projectRef, lastInstant, lastOrdering, (), rev)
    }

  /**
    * Creates a projection allowing to compute the last instant and the last ordering values for every project.
    *
    * This value is then used for passivation and the reactivation of other projections (ex: those related to indexing)
    *
    * @param supervisor
    *   the supervisor which will supervise the projection
    * @param store
    *   the store allowing to fetch and save project last updates
    * @param xas
    *   doobie
    * @param batchConfig
    *   a batch configuration for fetching the elems and for the sink
    */
  def apply(
      supervisor: Supervisor,
      store: ProjectLastUpdateStore,
      xas: Transactors,
      batchConfig: BatchConfig
  ): IO[ProjectLastUpdateWrites] = {
    // We build an elem streaming based on a delay
    val refreshStrategy = RefreshStrategy.Delay(batchConfig.maxInterval)
    val elemStream      = (offset: Offset) =>
      StreamingQuery(offset, query(_, batchConfig.maxElements), _.offset, refreshStrategy, xas)
    apply(supervisor, store, elemStream, batchConfig)
  }

  def apply(
      supervisor: Supervisor,
      store: ProjectLastUpdateStore,
      elemStream: Offset => ElemStream[Unit],
      batchConfig: BatchConfig
  ): IO[ProjectLastUpdateWrites] = {
    val source             = Source { (offset: Offset) => elemStream(offset) }
    val sink               = ProjectLastUpdatesSink(store, batchConfig)
    val compiledProjection = CompiledProjection.compile(
      projectionMetadata,
      ExecutionStrategy.PersistentSingleNode,
      source,
      sink
    )

    IO.fromEither(compiledProjection)
      .flatMap { projection =>
        supervisor.run(projection)
      }
      .as(dummy)
  }

}
// $COVERAGE-ON$
