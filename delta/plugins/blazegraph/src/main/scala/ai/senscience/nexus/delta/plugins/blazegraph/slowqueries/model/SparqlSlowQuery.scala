package ai.senscience.nexus.delta.plugins.blazegraph.slowqueries.model

import ai.senscience.nexus.delta.kernel.search.Pagination
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.contexts
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.rdf.query.SparqlQuery
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.model.search.SearchResults
import ai.senscience.nexus.delta.sdk.model.search.SearchResults.searchResultsJsonLdEncoder
import ai.senscience.nexus.delta.sdk.views.ViewRef
import ai.senscience.nexus.delta.sourcing.implicits.*
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import doobie.Read
import doobie.postgres.implicits.*
import io.circe.syntax.KeyOps
import io.circe.{Encoder, Json, JsonObject}
import org.http4s.Uri

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

  type SparqlSlowQueryResults = SearchResults[SparqlSlowQuery]

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

  private val sparqlSlowQueryContext: ContextValue = ContextValue(contexts.error)

  implicit private def sparqlSlowQueryEncoder(implicit baseUri: BaseUri): Encoder.AsObject[SparqlSlowQuery] = {
    import ai.senscience.nexus.delta.sdk.implicits.*
    Encoder.AsObject.instance { query =>
      JsonObject(
        "view"     := query.view,
        "query"    := query.query.value,
        "failed"   := query.failed,
        "duration" := query.duration.toMillis,
        "subject"  := query.subject
      )

    }
  }

  def sparqlSlowQueryJsonLdEncoder(pagination: Pagination, uri: Uri)(implicit
      baseUri: BaseUri
  ): JsonLdEncoder[SparqlSlowQueryResults] =
    searchResultsJsonLdEncoder[SparqlSlowQuery](sparqlSlowQueryContext, pagination, uri)
}
