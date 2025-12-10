package ai.senscience.nexus.delta.sourcing.query

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.kernel.utils.UUIDF
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sourcing.config.ElemQueryConfig
import ai.senscience.nexus.delta.sourcing.implicits.*
import ai.senscience.nexus.delta.sourcing.model.{EntityType, Label, ProjectRef}
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.query.ElemStreaming.QueryStatus.{Ongoing, Suspended}
import ai.senscience.nexus.delta.sourcing.query.ElemStreaming.{logger, newState, QueryStatus}
import ai.senscience.nexus.delta.sourcing.query.RefreshOrStop.RefreshOutcome
import ai.senscience.nexus.delta.sourcing.query.RefreshOrStop.RefreshOutcome.{Delayed, OutOfPassivation, Passivated}
import ai.senscience.nexus.delta.sourcing.query.StreamingQuery.{logQuery, stateFilter, typesSqlArray}
import ai.senscience.nexus.delta.sourcing.stream.*
import ai.senscience.nexus.delta.sourcing.stream.Elem.{DroppedElem, SuccessElem}
import ai.senscience.nexus.delta.sourcing.{Scope, Transactors}
import cats.effect.IO
import doobie.Fragments
import doobie.postgres.implicits.*
import doobie.syntax.all.*
import doobie.util.query.Query0
import fs2.{Chunk, Stream}
import io.circe.Json

import java.time.Instant
import java.util.UUID
import scala.collection.mutable.ListBuffer

/**
  * Allow to stream elements from the database providing advanced configuration than the regular StreamingQuery
  * operations
  */
final class ElemStreaming(
    xas: Transactors,
    ongoingQueries: OngoingQueries,
    entityTypeFilter: EntityTypeFilter,
    queryConfig: ElemQueryConfig,
    projectActivity: ProjectActivity
)(using uuidF: UUIDF) {

  private val batchSize = queryConfig.batchSize

  /**
    * The stopping alternative for this elem streaming
    */
  def stopping: ElemStreaming = ElemStreaming.stopping(xas, ongoingQueries, entityTypeFilter, queryConfig)

  /**
    * Get information about the remaining elements to stream
    * @param scope
    *   the scope for the query
    * @param selectFilter
    *   what to filter for
    * @param start
    *   the offset to start from
    */
  def remaining(scope: Scope, selectFilter: SelectFilter, start: Offset): IO[Option[RemainingElems]] =
    StreamingQuery.remaining(scope, entityTypeFilter, selectFilter, start, xas)

  /**
    * Streams states and tombstones as [[Elem]] s without fetching the state value.
    *
    * Tombstones are translated as [[DroppedElem]].
    *
    * The stream termination depends on the provided [[ElemQueryConfig]]
    *
    * @param scope
    *   the scope of the states / tombstones
    * @param start
    *   the offset to start with
    * @param selectFilter
    *   what to filter for
    */
  def apply(
      scope: Scope,
      start: Offset,
      selectFilter: SelectFilter
  ): ElemStream[Unit] = {
    val refresh: RefreshOrStop                    = RefreshOrStop(scope, queryConfig, projectActivity)
    def query(offset: Offset): Query0[Elem[Unit]] = {
      sql"""((SELECT 'newState', type, id, org, project, instant, ordering, rev
           |FROM public.scoped_states
           |${stateEntityFilter(scope, offset, selectFilter)}
           |ORDER BY ordering
           |LIMIT $batchSize)
           |UNION ALL
           |(SELECT 'tombstone', type, id, org, project, instant, ordering, -1
           |FROM public.scoped_tombstones
           |${tombstoneFilter(scope, offset, selectFilter)}
           |ORDER BY ordering
           |LIMIT $batchSize)
           |ORDER BY ordering)
           |LIMIT $batchSize
           |""".stripMargin.query[(String, EntityType, Iri, Label, Label, Instant, Long, Int)].map {
        case (`newState`, entityType, id, org, project, instant, offset, rev) =>
          SuccessElem(entityType, id, ProjectRef(org, project), instant, Offset.at(offset), (), rev)
        case (_, entityType, id, org, project, instant, offset, rev)          =>
          DroppedElem(entityType, id, ProjectRef(org, project), instant, Offset.at(offset), rev)
      }
    }
    execute[Unit](start, query, refresh)
  }

  /**
    * Streams states and tombstones as [[Elem]] s.
    *
    * State values are decoded via the provided function. If the function succeeds they will be streamed as
    * [[SuccessElem[A]] ]. If the function fails, they will be streamed as FailedElem
    *
    * Tombstones are translated as [[DroppedElem]].
    *
    * The stream termination depends on the provided [[ElemQueryConfig]]
    *
    * @param scope
    *   the scope for the query
    * @param start
    *   the offset to start with
    * @param selectFilter
    *   what to filter for
    * @param decodeValue
    *   the function to decode states
    */
  def apply[A](
      scope: Scope,
      start: Offset,
      selectFilter: SelectFilter,
      decodeValue: (EntityType, Json) => IO[A]
  ): ElemStream[A] = {
    def query(offset: Offset): Query0[Elem[Json]] = {
      sql"""((SELECT 'newState', type, id, org, project, value, instant, ordering, rev
           |FROM public.scoped_states
           |${stateEntityFilter(scope, offset, selectFilter)}
           |ORDER BY ordering
           |LIMIT $batchSize)
           |UNION ALL
           |(SELECT 'tombstone', type, id, org, project, null, instant, ordering, -1
           |FROM public.scoped_tombstones
           |${tombstoneFilter(scope, offset, selectFilter)}
           |ORDER BY ordering
           |LIMIT $batchSize)
           |ORDER BY ordering)
           |LIMIT $batchSize
           |""".stripMargin.query[(String, EntityType, Iri, Label, Label, Option[Json], Instant, Long, Int)].map {
        case (`newState`, entityType, id, org, project, Some(json), instant, offset, rev) =>
          SuccessElem(entityType, id, ProjectRef(org, project), instant, Offset.at(offset), json, rev)
        case (_, entityType, id, org, project, _, instant, offset, rev)                   =>
          DroppedElem(entityType, id, ProjectRef(org, project), instant, Offset.at(offset), rev)
      }
    }

    val refresh: RefreshOrStop = RefreshOrStop(scope, queryConfig, projectActivity)
    execute[Json](start, query, refresh)
      .evalMapChunk { e =>
        e.evalMap { value =>
          decodeValue(e.tpe, value).onError { case err =>
            logger.error(err)(
              s"An error occurred while decoding value with id '${e.id}' of type '${e.tpe}' in '$scope'."
            )
          }
        }
      }
  }

  /**
    * Streams the results of a query starting with the provided offset.
    *
    * The stream termination depends on the provided [[ElemQueryConfig]].
    *
    * @param start
    *   the offset to start with
    * @param query
    *   the query to execute depending on the offset
    * @param refresh
    *   whether to continue or stop after the stream completion
    */
  private def execute[A](
      start: Offset,
      query: Offset => Query0[Elem[A]],
      refresh: RefreshOrStop
  ): ElemStream[A] = {

    def onRefresh(queryStatus: QueryStatus): IO[Option[(ElemChunk[A], QueryStatus)]] =
      refresh.run(queryStatus.refreshOutcome).map {
        case RefreshOutcome.Stopped         => None
        case other: RefreshOutcome.Continue =>
          Some(Chunk.empty[Elem[A]] -> Suspended(queryStatus.uuid, queryStatus.offset, other))
      }

    def execQuery(queryStatus: QueryStatus) =
      ongoingQueries.tryRun(queryStatus)(
        (queryStatus: QueryStatus) =>
          query(queryStatus.offset)
            .to[List]
            .transact(xas.streaming)
            .flatMap { elems =>
              elems.lastOption.fold(onRefresh(queryStatus)) { last =>
                IO.pure(Some((dropDuplicates(elems), Ongoing(queryStatus.uuid, last.offset))))
              }
            },
        (queryStatus: QueryStatus) =>
          IO.sleep(queryConfig.delay) >>
            IO.some(Chunk.empty[Elem[A]] -> queryStatus)
      )

    Stream.eval(uuidF()).flatMap { uuid =>
      Stream
        .unfoldChunkEval[IO, QueryStatus, Elem[A]](Ongoing(uuid, start)) {
          case ongoing: Ongoing                                          =>
            execQuery(ongoing)
          case status @ QueryStatus.Suspended(uuid, _, Delayed)          =>
            execQuery(status)
          case status @ QueryStatus.Suspended(uuid, _, OutOfPassivation) =>
            execQuery(status)
          case status @ QueryStatus.Suspended(uuid, _, Passivated)       =>
            onRefresh(status)
        }
        .onFinalizeCase(logQuery(query(start)))
    }
  }

  // Looks for duplicates and keep the last occurrence
  private def dropDuplicates[A](elems: List[Elem[A]]): ElemChunk[A] = {
    val (_, buffer) = elems.foldRight((Set.empty[(ProjectRef, Iri)], new ListBuffer[Elem[A]])) {
      case (elem, (seen, buffer)) =>
        val key = (elem.project, elem.id)
        if seen.contains(key) then (seen, buffer)
        else (seen + key, buffer.prepend(elem))
    }
    Chunk.from(buffer)
  }

  private def stateEntityFilter(scope: Scope, offset: Offset, selectFilter: SelectFilter) =
    Fragments.whereAndOpt(
      entityTypeFilter.asFragment,
      stateFilter(scope, offset, selectFilter)
    )

  private def tombstoneFilter(scope: Scope, offset: Offset, selectFilter: SelectFilter) = {
    val typeFragment  =
      selectFilter.types.asRestrictedTo.map(includedTypes => fr"cause -> 'types' ??| ${typesSqlArray(includedTypes)}")
    val causeFragment = Fragments.orOpt(Some(fr"cause->>'deleted' = 'true'"), typeFragment)
    Fragments.whereAndOpt(
      entityTypeFilter.asFragment,
      scope.asFragment,
      offset.asFragment,
      selectFilter.tag.asFragment,
      causeFragment
    )
  }
}

object ElemStreaming {

  private val logger = Logger[ElemStreaming]

  private val newState = "newState"

  /**
    * Query status of a streaming query
    */
  sealed trait QueryStatus {

    def uuid: UUID

    def offset: Offset

    def refreshOutcome: Option[RefreshOutcome.Continue]
  }

  object QueryStatus {

    /**
      * Streaming query currently running because there is still data to index and the ongoing set allowed it to run
      */
    final case class Ongoing(uuid: UUID, offset: Offset) extends QueryStatus {
      override def refreshOutcome: Option[RefreshOutcome.Continue] = None

      def suspended(reason: RefreshOutcome.Continue) = Suspended(uuid, offset, reason)
    }

    /**
      * Streaming query currently suspended, waiting for more data and or an available slot in the ongoing set
      */
    final case class Suspended(uuid: UUID, offset: Offset, reason: RefreshOutcome.Continue) extends QueryStatus {
      def refreshOutcome: Option[RefreshOutcome.Continue] = Some(reason)
    }
  }

  /**
    * Constructs an elem streaming with a stopping strategy
    */
  def stopping(
      xas: Transactors,
      ongoingSet: OngoingQueries,
      entityTypeFilter: EntityTypeFilter,
      config: ElemQueryConfig
  )(using UUIDF): ElemStreaming = {
    val eqc      = ElemQueryConfig.StopConfig(config.maxOngoing, config.batchSize, config.delay)
    val activity = ProjectActivity.noop
    new ElemStreaming(xas, ongoingSet, entityTypeFilter, eqc, activity)
  }
}
