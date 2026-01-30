package ai.senscience.nexus.delta.sourcing.state

import ai.senscience.nexus.delta.sourcing.implicits.given
import ai.senscience.nexus.delta.sourcing.model.{EntityType, ProjectRef}
import ai.senscience.nexus.delta.sourcing.state.State.EphemeralState
import ai.senscience.nexus.delta.sourcing.{Serializer, Transactors}
import cats.effect.IO
import cats.syntax.all.*
import doobie.*
import doobie.postgres.implicits.*
import doobie.syntax.all.*

import scala.concurrent.duration.FiniteDuration

/**
  * Allows to save/fetch [[EphemeralState]] from the database
  */
trait EphemeralStateStore[Id, S <: EphemeralState] {

  /**
    * Persist the state
    */
  def save(state: S): ConnectionIO[Unit]

  /**
    * Returns the state
    */
  def get(ref: ProjectRef, id: Id): IO[Option[S]]
}

object EphemeralStateStore {

  def apply[Id, S <: EphemeralState](
      tpe: EntityType,
      serializer: Serializer[Id, S],
      ttl: FiniteDuration,
      xas: Transactors
  ): EphemeralStateStore[Id, S] =
    new EphemeralStateStore[Id, S] {
      private given Put[Id] = serializer.putId
      private given Get[S]  = serializer.getValue
      private given Put[S]  = serializer.putValue

      override def save(state: S): doobie.ConnectionIO[Unit] = {
        sql"""
           | INSERT INTO public.ephemeral_states (
           |  type,
           |  org,
           |  project,
           |  id,
           |  value,
           |  instant,
           |  expires
           | )
           | VALUES (
           |  $tpe,
           |  ${state.organization},
           |  ${state.project.project},
           |  ${state.id},
           |  $state,
           |  ${state.updatedAt},
           |  ${state.updatedAt.plusMillis(ttl.toMillis)}
           | )
            """.stripMargin
      }.update.run.void

      override def get(ref: ProjectRef, id: Id): IO[Option[S]] =
        sql"""SELECT value FROM public.ephemeral_states WHERE type = $tpe AND org = ${ref.organization} AND project = ${ref.project}  AND id = $id"""
          .query[S]
          .option
          .transact(xas.read)
    }

}
