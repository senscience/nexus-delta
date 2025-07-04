package ai.senscience.nexus.delta.sourcing.state

import ai.senscience.nexus.delta.kernel.error.ThrowableValue
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sourcing.config.QueryConfig
import ai.senscience.nexus.delta.sourcing.implicits.*
import ai.senscience.nexus.delta.sourcing.model.Tag.Latest
import ai.senscience.nexus.delta.sourcing.model.{EntityType, ProjectRef, Tag}
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.query.{RefreshStrategy, StreamingQuery}
import ai.senscience.nexus.delta.sourcing.state.ScopedStateStore.StateNotFound.{TagNotFound, UnknownState}
import ai.senscience.nexus.delta.sourcing.state.State.ScopedState
import ai.senscience.nexus.delta.sourcing.stream.{Elem, SuccessElemStream}
import ai.senscience.nexus.delta.sourcing.{Scope, Serializer, Transactors}
import cats.effect.IO
import cats.implicits.*
import doobie.*
import doobie.free.connection
import doobie.postgres.implicits.*
import doobie.syntax.all.*
import io.circe.Decoder

/**
  * Allows to save/fetch [[ScopedState]] from the database
  */
trait ScopedStateStore[Id, S <: ScopedState] {

  /**
    * @param state
    *   The state to persist
    * @param tag
    *   The tag of the state
    * @return
    *   A [[ConnectionIO]] describing how to persist the event.
    */
  def save(state: S, tag: Tag): ConnectionIO[Unit]

  /** Persist the state using the `latest` tag */
  def save(state: S): ConnectionIO[Unit] = save(state, Latest)

  /**
    * Delete the state for the given tag
    */
  def delete(project: ProjectRef, id: Id, tag: Tag): ConnectionIO[Unit]

  /**
    * Returns the latest state from the write nodes to get a stronger consistency when the Postgres works in a
    * replicated fashion
    */
  def getWrite(project: ProjectRef, id: Id): IO[S]

  /**
    * Returns the latest state from any node
    */
  def getRead(project: ProjectRef, id: Id): IO[S]

  /**
    * Returns the state at the given tag from any node
    */
  def getRead(project: ProjectRef, id: Id, tag: Tag): IO[S]

  def get(project: ProjectRef, id: Id): ConnectionIO[S]

  /**
    * Fetches latest states from the given type from the provided offset.
    *
    * The stream is completed when it reaches the end.
    * @param scope
    *   to filter returned states
    * @param offset
    *   the offset
    */
  final def currentStates(scope: Scope, offset: Offset): SuccessElemStream[S] =
    currentStates(scope, Latest, offset)

  /**
    * Fetches states from the given type with the given tag from the provided offset.
    *
    * The stream is completed when it reaches the end.
    * @param scope
    *   to filter returned states
    * @param tag
    *   only states with this tag will be selected
    * @param offset
    *   the offset
    */
  def currentStates(scope: Scope, tag: Tag, offset: Offset): SuccessElemStream[S]

  /**
    * Fetches latest states from the given type from the provided offset
    *
    * The stream is not completed when it reaches the end of the existing events, but it continues to push new events
    * when new events are persisted.
    *
    * @param scope
    *   to filter returned states
    * @param offset
    *   the offset
    */
  final def states(scope: Scope, offset: Offset): SuccessElemStream[S] =
    states(scope, Latest, offset)

  /**
    * Fetches states from the given type with the given tag from the provided offset
    *
    * The stream is not completed when it reaches the end of the existing events, but it continues to push new events
    * when new states are persisted.
    *
    * @param scope
    *   to filter returned states
    * @param tag
    *   only states with this tag will be selected
    * @param offset
    *   the offset
    */
  def states(scope: Scope, tag: Tag, offset: Offset): SuccessElemStream[S]

}

object ScopedStateStore {

  sealed private[sourcing] trait StateNotFound extends ThrowableValue

  private[sourcing] object StateNotFound {
    sealed trait UnknownState      extends StateNotFound
    final case object UnknownState extends UnknownState
    sealed trait TagNotFound       extends StateNotFound
    final case object TagNotFound  extends TagNotFound
  }

  def apply[Id, S <: ScopedState](
      tpe: EntityType,
      serializer: Serializer[Id, S],
      config: QueryConfig,
      xas: Transactors
  ): ScopedStateStore[Id, S] = new ScopedStateStore[Id, S] {
    implicit val putId: Put[Id]      = serializer.putId
    implicit val getValue: Get[S]    = serializer.getValue
    implicit val putValue: Put[S]    = serializer.putValue
    implicit val decoder: Decoder[S] = serializer.codec

    override def save(state: S, tag: Tag): doobie.ConnectionIO[Unit] =
      sql"SELECT 1 FROM scoped_states WHERE type = $tpe AND org = ${state.organization} AND project = ${state.project.project}  AND id = ${state.id} AND tag = $tag"
        .query[Int]
        .option
        .flatMap {
          _.fold(sql"""
                      | INSERT INTO scoped_states (
                      |  type,
                      |  org,
                      |  project,
                      |  id,
                      |  tag,
                      |  rev,
                      |  value,
                      |  deprecated,
                      |  instant
                      | )
                      | VALUES (
                      |  $tpe,
                      |  ${state.organization},
                      |  ${state.project.project},
                      |  ${state.id},
                      |  $tag,
                      |  ${state.rev},
                      |  $state,
                      |  ${state.deprecated},
                      |  ${state.updatedAt}
                      | )
            """.stripMargin) { _ =>
            sql"""
                 | UPDATE scoped_states SET
                 |  rev = ${state.rev},
                 |  value = $state,
                 |  deprecated = ${state.deprecated},
                 |  instant = ${state.updatedAt},
                 |  ordering = (select nextval('state_offset'))
                 | WHERE
                 |  type = $tpe AND
                 |  org = ${state.organization} AND
                 |  project = ${state.project.project} AND
                 |  id =  ${state.id} AND
                 |  tag = $tag
            """.stripMargin
          }.update.run.void
        }

    override def delete(project: ProjectRef, id: Id, tag: Tag): ConnectionIO[Unit] =
      sql"""DELETE FROM scoped_states WHERE type = $tpe AND org = ${project.organization} AND project = ${project.project}  AND id = $id AND tag = $tag""".stripMargin.update.run.void

    private def exists(ref: ProjectRef, id: Id): ConnectionIO[Boolean] =
      sql"""SELECT id FROM scoped_states WHERE type = $tpe AND org = ${ref.organization} AND project = ${ref.project} AND id = $id LIMIT 1"""
        .query[Iri]
        .option
        .map(_.isDefined)

    override def getWrite(project: ProjectRef, id: Id): IO[S] =
      get(project, id).transact(xas.write)

    override def getRead(project: ProjectRef, id: Id): IO[S] =
      get(project, id).transact(xas.read)

    override def get(project: ProjectRef, id: Id): ConnectionIO[S] =
      ScopedStateGet.latest[Id, S](tpe, project, id).flatMap {
        case Some(s) => s.pure[ConnectionIO]
        case None    => connection.raiseError(UnknownState)
      }

    override def getRead(project: ProjectRef, id: Id, tag: Tag): IO[S] = {
      for {
        value  <- ScopedStateGet[Id, S](tpe, project, id, tag)
        exists <- value.fold(exists(project, id))(_ => true.pure[ConnectionIO])
      } yield value -> exists
    }.transact(xas.read).flatMap { case (s, exists) =>
      IO.fromOption(s)(if (exists) TagNotFound else UnknownState)
    }

    private def states(
        scope: Scope,
        tag: Tag,
        offset: Offset,
        strategy: RefreshStrategy
    ): SuccessElemStream[S] =
      StreamingQuery[Elem.SuccessElem[S]](
        offset,
        offset =>
          // format: off
          sql"""SELECT type, id, value, rev, instant, ordering, org, project FROM public.scoped_states
               |${Fragments.whereAndOpt(Some(fr"type = $tpe"), scope.asFragment, Some(fr"tag = $tag"), offset.asFragment)}
               |ORDER BY ordering
               |LIMIT ${config.batchSize}""".stripMargin.query[Elem.SuccessElem[S]],
        _.offset,
        strategy,
        xas
      )

    override def currentStates(scope: Scope, tag: Tag, offset: Offset): SuccessElemStream[S] =
      states(scope, tag, offset, RefreshStrategy.Stop)

    override def states(scope: Scope, tag: Tag, offset: Offset): SuccessElemStream[S] =
      states(scope, tag, offset, config.refreshStrategy)
  }

}
