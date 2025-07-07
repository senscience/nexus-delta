package ai.senscience.nexus.delta.plugins.elasticsearch.routes

import ai.senscience.nexus.delta.plugins.elasticsearch.metrics.FetchHistory
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.directives.AuthDirectives
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.{emit, projectRef}
import ai.senscience.nexus.delta.sdk.directives.UriDirectives.iriSegment
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.marshalling.RdfMarshalling
import ai.senscience.nexus.delta.sdk.model.search.SearchResults
import ai.senscience.nexus.delta.sdk.model.search.SearchResults.searchResultsEncoder
import ai.senscience.nexus.delta.sdk.permissions.Permissions.resources.read as Read
import akka.http.scaladsl.server.Route
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, JsonObject}

/**
  * Routes allowing to get the history of events for resources
  */
class ElasticSearchHistoryRoutes(identities: Identities, aclCheck: AclCheck, fetchHistory: FetchHistory)(implicit
    cr: RemoteContextResolution,
    ordering: JsonKeyOrdering
) extends AuthDirectives(identities, aclCheck)
    with RdfMarshalling {
  implicit private val searchEncoder: Encoder.AsObject[SearchResults[JsonObject]] = searchResultsEncoder(_ => None)

  def routes: Route =
    handleExceptions(ElasticSearchExceptionHandler.apply) {
      pathPrefix("history") {
        pathPrefix("resources") {
          extractCaller { implicit caller =>
            projectRef.apply { project =>
              authorizeFor(project, Read).apply {
                (get & iriSegment & pathEndOrSingleSlash) { id =>
                  emit(fetchHistory.history(project, id).map(_.asJson))
                }
              }
            }
          }
        }
      }
    }
}
