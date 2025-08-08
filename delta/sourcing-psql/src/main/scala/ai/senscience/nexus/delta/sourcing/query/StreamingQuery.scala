package ai.senscience.nexus.delta.sourcing.query

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sourcing.implicits.*
import ai.senscience.nexus.delta.sourcing.model.{EntityType, IriFilter}
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.stream.RemainingElems
import ai.senscience.nexus.delta.sourcing.{Scope, Transactors}
import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.kernel.Resource
import doobie.Fragments
import doobie.postgres.implicits.*
import doobie.syntax.all.*
import doobie.util.fragment.Fragment
import doobie.util.query.Query0
import fs2.{Chunk, Stream}

import java.sql.SQLException
import java.time.Instant

/**
  * Provide utility methods to stream results from the database according to a [[RefreshStrategy]].
  */
object StreamingQuery {

  private val logger = Logger[StreamingQuery.type]

  /**
    * Get information about the remaining elements to stream
    * @param scope
    *   the scope for the query
    * @param selectFilter
    *   what to filter for
    * @param xas
    *   the transactors
    */
  def remaining(
      scope: Scope,
      entityTypes: Option[NonEmptyList[EntityType]],
      selectFilter: SelectFilter,
      start: Offset,
      xas: Transactors
  ): IO[Option[RemainingElems]] = {
    val whereClause = Fragments.whereAndOpt(
      entityTypeFilter(entityTypes),
      stateFilter(scope, start, selectFilter)
    )
    sql"""SELECT count(ordering), max(instant)
         |FROM public.scoped_states
         |$whereClause
         |""".stripMargin
      .query[(Long, Option[Instant])]
      .map { case (count, maxInstant) =>
        maxInstant.map { m => RemainingElems(count, m) }
      }
      .unique
      .transact(xas.read)
  }

  /**
    * Return if the given resource is eligible to the stream
    */
  def offset(
      scope: Scope,
      entityTypes: Option[NonEmptyList[EntityType]],
      selectFilter: SelectFilter,
      id: Iri,
      xas: Transactors
  ): IO[Option[Offset]] = {
    val whereClause = Fragments.whereAndOpt(
      entityTypeFilter(entityTypes),
      stateFilter(scope, Offset.start, selectFilter),
      Some(fr"id = $id")
    )
    sql"""SELECT ordering
         |FROM public.scoped_states
         |$whereClause
         |""".stripMargin
      .query[Offset]
      .option
      .transact(xas.read)
  }

  /**
    * Streams the results of a query starting with the provided offset.
    *
    * The stream termination depends on the provided [[RefreshStrategy]].
    *
    * @param start
    *   the offset to start with
    * @param query
    *   the query to execute depending on the offset
    * @param extractOffset
    *   how to extract the offset from an [[A]] to be able to pursue the stream
    * @param refreshStrategy
    *   the refresh strategy
    * @param xas
    *   the transactors
    */
  def apply[A](
      start: Offset,
      query: Offset => Query0[A],
      extractOffset: A => Offset,
      refreshStrategy: RefreshStrategy,
      xas: Transactors
  ): Stream[IO, A] =
    Stream
      .unfoldChunkEval[IO, Offset, A](start) { offset =>
        query(offset).accumulate[Chunk].transact(xas.streaming).flatMap { elems =>
          elems.last.fold(refreshOrStop[A](refreshStrategy, offset)) { last =>
            IO.pure(Some((elems, extractOffset(last))))
          }
        }
      }
      .onFinalizeCase(logQuery(query(start)))

  private def refreshOrStop[A](refreshStrategy: RefreshStrategy, offset: Offset): IO[Option[(Chunk[A], Offset)]] =
    refreshStrategy match {
      case RefreshStrategy.Stop         => IO.none
      case RefreshStrategy.Delay(value) => IO.sleep(value) >> IO.pure(Some((Chunk.empty[A], offset)))
    }

  def logQuery[A](query: Query0[A]): Resource.ExitCase => IO[Unit] = {
    case Resource.ExitCase.Succeeded                    =>
      logger.debug(s"Reached the end of the evaluation of query '${query.sql}'.")
    case Resource.ExitCase.Errored(cause: SQLException) =>
      logger.error(cause)(
        s"Evaluation of query '${query.sql}' failed with error: ${cause.getMessage} (${cause.getErrorCode})."
      )
    case Resource.ExitCase.Errored(cause)               =>
      logger.debug(cause)(s"Evaluation of query '${query.sql}' failed with error: ${cause.getMessage}.")
    case Resource.ExitCase.Canceled                     =>
      logger.debug(s"Cancelled the evaluation of query '${query.sql}'.")
  }

  def stateFilter(scope: Scope, offset: Offset, selectFilter: SelectFilter): Option[doobie.Fragment] = {
    val typeFragment =
      selectFilter.types.asRestrictedTo.map(restriction => fr"value -> 'types' ??| ${typesSqlArray(restriction)}")
    Fragments.andOpt(
      scope.asFragment,
      offset.asFragment,
      selectFilter.tag.asFragment,
      typeFragment
    )
  }

  def entityTypeFilter(entityTypes: Option[NonEmptyList[EntityType]]): Option[doobie.Fragment] = entityTypes.map { e =>
    Fragments.in(fr"type", e)
  }

  def typesSqlArray(includedTypes: IriFilter.Include): Fragment =
    Fragment.const(s"ARRAY[${includedTypes.iris.map(t => s"'$t'").mkString(",")}]")

}
