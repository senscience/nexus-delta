package ai.senscience.nexus.delta.sourcing.tombstone

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sourcing.Transactors
import ai.senscience.nexus.delta.sourcing.implicits.*
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.{EntityType, Identity, Label, ProjectRef}
import ai.senscience.nexus.delta.sourcing.tombstone.EventTombstoneStore.{EventTombstone, Value}
import cats.effect.IO
import cats.syntax.all.*
import doobie.*
import doobie.postgres.implicits.*
import doobie.syntax.all.*
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveConfiguredCodec
import io.circe.syntax.EncoderOps
import io.circe.{Codec, Encoder}

import java.time.Instant

final class EventTombstoneStore(xas: Transactors) {

  def count: IO[Long] =
    sql"""SELECT count(*) FROM public.scoped_event_tombstones""".query[Long].unique.transact(xas.read)

  /**
    * Saves an event tombstone so that the event deletion can be communicated to downstream processes (SSEs/metrics
    * projection)
    */
  def save(tpe: EntityType, project: ProjectRef, id: Iri, subject: Subject): ConnectionIO[Unit] =
    sql"""
         | INSERT INTO public.scoped_event_tombstones (
         |  type,
         |  org,
         |  project,
         |  id,
         |  value
         | )
         | VALUES (
         |  $tpe,
         |  ${project.organization},
         |  ${project.project},
         |  $id,
         |  ${Value(subject).asJson}
         | )""".stripMargin.update.run.void

  /**
    * Returns a tombstone for the given project/id (meant for tests as in practice, a resource can be created and
    * deleted several times and get several tombstones)
    */
  def unsafeGet(project: ProjectRef, id: Iri): IO[Option[EventTombstone]] =
    sql"""|SELECT type, org, project, id, value, instant
          |FROM public.scoped_event_tombstones
          |WHERE org = ${project.organization} and project= ${project.project} and id = $id
          |""".stripMargin.query[EventTombstone].option.transact(xas.read)
}

object EventTombstoneStore {

  final case class EventTombstone(
      entityType: EntityType,
      project: ProjectRef,
      id: Iri,
      value: Value,
      instant: Instant
  )

  object EventTombstone {
    given Read[EventTombstone] = {
      given Get[Value] = pgDecoderGetT[Value]
      Read[(EntityType, Label, Label, Iri, Value, Instant)].map { case (tpe, org, project, id, cause, instant) =>
        EventTombstone(tpe, ProjectRef(org, project), id, cause, instant)
      }
    }
  }

  final case class Value(subject: Subject)

  object Value {
    given Codec[Value] = {
      given Encoder[Subject] = Identity.Database.subjectCodec
      given Configuration    = Configuration.default
      deriveConfiguredCodec[Value]
    }
  }

}
