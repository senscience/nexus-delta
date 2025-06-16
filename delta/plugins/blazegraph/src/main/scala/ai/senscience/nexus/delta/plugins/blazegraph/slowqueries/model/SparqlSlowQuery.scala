package ai.senscience.nexus.delta.plugins.blazegraph.slowqueries.model

import ai.senscience.nexus.delta.sdk.views.ViewRef
import ch.epfl.bluebrain.nexus.delta.rdf.IriOrBNode.Iri
import ch.epfl.bluebrain.nexus.delta.rdf.query.SparqlQuery
import ch.epfl.bluebrain.nexus.delta.sourcing.implicits.*
import ch.epfl.bluebrain.nexus.delta.sourcing.model.Identity.Subject
import ch.epfl.bluebrain.nexus.delta.sourcing.model.ProjectRef
import doobie.Read
import doobie.postgres.implicits.*
import io.circe.Json

import java.time.Instant
import scala.concurrent.duration.{DurationLong, FiniteDuration}

/**
  * A blazegraph query which took too long
  * @param view
  *   the view
  * @param query
  *   the query
  * @param failed
  *   whether the query failed
  * @param duration
  *   how long the query took
  * @param instant
  *   when the query finished
  * @param subject
  *   who ran the query
  */
final case class SparqlSlowQuery(
    view: ViewRef,
    query: SparqlQuery,
    failed: Boolean,
    duration: FiniteDuration,
    instant: Instant,
    subject: Subject
)

object SparqlSlowQuery {

  implicit val read: Read[SparqlSlowQuery] = {
    Read[(ProjectRef, Iri, Instant, Long, Json, String, Boolean)].map {
      case (project, viewId, occurredAt, duration, subject, query, failed) =>
        SparqlSlowQuery(
          ViewRef(project, viewId),
          SparqlQuery(query),
          failed,
          duration.milliseconds,
          occurredAt,
          subject.as[Subject] match {
            case Right(value) => value
            case Left(e)      => throw e
          }
        )
    }
  }
}
