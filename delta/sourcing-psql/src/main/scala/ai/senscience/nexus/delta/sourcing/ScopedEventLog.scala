package ai.senscience.nexus.delta.sourcing

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.sourcing.EvaluationError.EvaluationTagFailure
import ai.senscience.nexus.delta.sourcing.ScopedEntityDefinition.Tagger
import ai.senscience.nexus.delta.sourcing.config.EventLogConfig
import ai.senscience.nexus.delta.sourcing.event.Event.ScopedEvent
import ai.senscience.nexus.delta.sourcing.event.ScopedEventStore
import ai.senscience.nexus.delta.sourcing.model.EntityDependency.DependsOn
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.Tag.UserTag
import ai.senscience.nexus.delta.sourcing.model.{EntityType, ProjectRef, Tag}
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.state.ScopedStateStore
import ai.senscience.nexus.delta.sourcing.state.ScopedStateStore.StateNotFound.{TagNotFound, UnknownState}
import ai.senscience.nexus.delta.sourcing.state.State.ScopedState
import ai.senscience.nexus.delta.sourcing.stream.SuccessElemStream
import ai.senscience.nexus.delta.sourcing.tombstone.{EventTombstoneStore, StateTombstoneStore}
import cats.effect.IO
import cats.syntax.all.*
import doobie.*
import doobie.postgres.sqlstate
import doobie.syntax.all.*
import fs2.Stream

import java.sql.SQLException
import scala.concurrent.duration.FiniteDuration

/**
  * Event log for project-scoped entities that can be controlled through commands;
  *
  * Successful commands result in state transitions. If we use a persistent implementation, new events are also appended
  * to the event log.
  *
  * Unsuccessful commands result in rejections returned to the caller context without any events being generated or
  * state transitions applied.
  */
trait ScopedEventLog[Id, S <: ScopedState, Command, E <: ScopedEvent, Rejection <: Throwable]
    extends ScopedEventLogReadOnly[Id, S, Rejection] {

  /**
    * Evaluates the argument __command__ in the context of entity identified by __id__.
    * @param project
    *   the project the entity belongs in
    * @param id
    *   the entity identifier
    * @param command
    *   the command to evaluate
    * @return
    *   the newly generated state and appended event if the command was evaluated successfully, or the rejection of the
    *   __command__ otherwise
    */
  def evaluate(project: ProjectRef, id: Id, command: Command): IO[(E, S)]

  /**
    * Tests the evaluation the argument __command__ in the context of entity identified by __id__, without applying any
    * changes to the state or event log of the entity regardless of the outcome of the command evaluation.
    *
    * @param project
    *   the project the entity belongs in
    * @param id
    *   the entity identifier
    * @param command
    *   the command to evaluate
    * @return
    *   the state and event that would be generated in if the command was tested for evaluation successfully, or the
    *   rejection of the __command__ in otherwise
    */
  def dryRun(project: ProjectRef, id: Id, command: Command): IO[(E, S)]

  /**
    * Deletes the entity identified by __id__
    * @param project
    *   the project the entity belongs in
    * @param id
    *   the entity identifier
    * @return
    */
  def delete[R <: Rejection](project: ProjectRef, id: Id, notFound: => R)(implicit subject: Subject): IO[Unit]

}

object ScopedEventLog {

  private val logger = Logger[ScopedEventLog.type]

  private val noop: ConnectionIO[Unit] = doobie.free.connection.unit

  def apply[Id, S <: ScopedState, Command, E <: ScopedEvent, Rejection <: Throwable](
      definition: ScopedEntityDefinition[Id, S, Command, E, Rejection],
      config: EventLogConfig,
      xas: Transactors
  ): ScopedEventLog[Id, S, Command, E, Rejection] =
    apply(
      definition.tpe,
      ScopedEventStore(definition.tpe, definition.eventSerializer, config.queryConfig),
      ScopedStateStore(definition.tpe, definition.stateSerializer, config.queryConfig, xas),
      definition.stateMachine,
      definition.onUniqueViolation,
      definition.tagger,
      definition.extractDependencies,
      config.maxDuration,
      xas
    )

  def apply[Id, S <: ScopedState, Command, E <: ScopedEvent, Rejection <: Throwable](
      entityType: EntityType,
      eventStore: ScopedEventStore[Id, E],
      stateStore: ScopedStateStore[Id, S],
      stateMachine: StateMachine[S, Command, E],
      onUniqueViolation: (Id, Command) => Rejection,
      tagger: Tagger[S, E],
      extractDependencies: S => Option[Set[DependsOn]],
      maxDuration: FiniteDuration,
      xas: Transactors
  ): ScopedEventLog[Id, S, Command, E, Rejection] =
    new ScopedEventLog[Id, S, Command, E, Rejection] {

      private val eventTombstoneStore = new EventTombstoneStore(xas)
      private val stateTombstoneStore = new StateTombstoneStore(xas)

      override def stateOr[R <: Rejection](ref: ProjectRef, id: Id, notFound: => R): IO[S] =
        stateStore.getRead(ref, id).adaptError(_ => notFound)

      override def stateOr[R <: Rejection](
          ref: ProjectRef,
          id: Id,
          tag: Tag,
          notFound: => R,
          tagNotFound: => R
      ): IO[S] = {
        stateStore.getRead(ref, id, tag).adaptError {
          case UnknownState => notFound
          case TagNotFound  => tagNotFound
        }
      }

      override def stateOr[R <: Rejection](
          ref: ProjectRef,
          id: Id,
          rev: Int,
          notFound: => R,
          invalidRevision: (Int, Int) => R
      ): IO[S] =
        stateMachine.computeState(eventStore.history(ref, id, rev).transact(xas.read)).flatMap {
          case Some(s) if s.rev == rev => IO.pure(s)
          case Some(s)                 => IO.raiseError(invalidRevision(rev, s.rev))
          case None                    => IO.raiseError(notFound)
        }

      override def evaluate(project: ProjectRef, id: Id, command: Command): IO[(E, S)] = {

        def newTaggedState(event: E, state: S): IO[Option[(UserTag, S)]] =
          tagger.tagWhen(event) match {
            case Some((tag, rev)) if rev == state.rev =>
              IO.some(tag -> state)
            case Some((tag, rev))                     =>
              stateMachine
                .computeState(eventStore.history(project, id, Some(rev)).transact(xas.write))
                .flatTap {
                  case stateOpt if !stateOpt.exists(_.rev == rev) =>
                    IO.raiseError(EvaluationTagFailure(command, stateOpt.map(_.rev)))
                  case _                                          => IO.unit
                }
                .map(_.map(tag -> _))
            case None                                 => IO.none
          }

        def deleteTag(event: E, state: S): ConnectionIO[Unit] = tagger.untagWhen(event).fold(noop) { tag =>
          stateStore.delete(project, id, tag) >>
            stateTombstoneStore.save(entityType, state, tag)
        }

        def updateDependencies(state: S) =
          extractDependencies(state).fold(noop) { dependencies =>
            EntityDependencyStore
              .delete(project, state.id) >> EntityDependencyStore.save(project, state.id, dependencies)
          }

        def persist(event: E, original: Option[S], newState: S): IO[Unit] = {

          def queries(newTaggedState: Option[(UserTag, S)]) =
            for {
              _ <- stateTombstoneStore.save(entityType, original, newState)
              _ <- eventStore.save(event)
              _ <- stateStore.save(newState)
              _ <- newTaggedState.traverse { case (tag, taggedState) =>
                     stateStore.save(taggedState, tag)
                   }
              _ <- deleteTag(event, newState)
              _ <- updateDependencies(newState)
            } yield ()

          {
            for {
              taggedState <- newTaggedState(event, newState)
              res         <- queries(taggedState).transact(xas.write)
            } yield res
          }.recoverWith {
            case sql: SQLException if isUniqueViolation(sql) =>
              logger.error(sql)(
                s"A unique constraint violation occurred when persisting an event for  '$id' in project '$project' and rev ${event.rev}."
              ) >>
                IO.raiseError(onUniqueViolation(id, command))
            case other                                       =>
              logger.error(other)(
                s"An error occurred when persisting an event for '$id' in project '$project' and rev ${event.rev}."
              ) >>
                IO.raiseError(other)
          }
        }

        for {
          originalState <- stateStore.getWrite(project, id).redeem(_ => None, Some(_))
          result        <- stateMachine.evaluate(originalState, command, maxDuration)
          _             <- persist(result._1, originalState, result._2)
        } yield result
      }

      private def isUniqueViolation(sql: SQLException) =
        sql.getSQLState == sqlstate.class23.UNIQUE_VIOLATION.value

      override def dryRun(project: ProjectRef, id: Id, command: Command): IO[(E, S)] =
        stateStore.getWrite(project, id).redeem(_ => None, Some(_)).flatMap { state =>
          stateMachine.evaluate(state, command, maxDuration)
        }

      override def delete[R <: Rejection](project: ProjectRef, id: Id, notFound: => R)(implicit
          subject: Subject
      ): IO[Unit] = {
        val queries = for {
          originalState <- stateStore.get(project, id).adaptError { case UnknownState => notFound }
          tags           = tagger.existingTags(originalState).fold(List.empty[Tag])(_.tags)
          _             <- stateTombstoneStore.save(entityType, originalState, tags)
          _             <- stateTombstoneStore.save(entityType, originalState, Tag.Latest)
          _             <- eventTombstoneStore.save(entityType, project, originalState.id, subject)
          _             <- tags.traverse { tag => stateStore.delete(project, id, tag) }
          _             <- stateStore.delete(project, id, Tag.Latest)
          _             <- EntityDependencyStore.delete(project, originalState.id)
        } yield ()

        queries.transact(xas.write)
      }

      override def currentStates(scope: Scope, offset: Offset): SuccessElemStream[S] =
        stateStore.currentStates(scope, offset)

      override def currentStates[T](scope: Scope, offset: Offset, f: S => T): Stream[IO, T] =
        currentStates(scope, offset).map { s => f(s.value) }

      override def states(scope: Scope, offset: Offset): SuccessElemStream[S] =
        stateStore.states(scope, offset)
    }

}
