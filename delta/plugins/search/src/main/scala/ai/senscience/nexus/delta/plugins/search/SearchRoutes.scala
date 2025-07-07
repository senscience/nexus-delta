package ai.senscience.nexus.delta.plugins.search

import ai.senscience.nexus.akka.marshalling.CirceUnmarshalling
import ai.senscience.nexus.delta.plugins.elasticsearch.routes.ElasticSearchExceptionHandler
import ai.senscience.nexus.delta.plugins.elasticsearch.routes.ElasticSearchViewsDirectives.extractQueryParams
import ai.senscience.nexus.delta.plugins.search.model.SearchConfig.NamedSuite
import ai.senscience.nexus.delta.plugins.search.model.SearchRejection.UnknownSuite
import ai.senscience.nexus.delta.plugins.search.model.{SearchConfig, SearchRejection}
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.directives.{AuthDirectives, DeltaDirectives}
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.marshalling.RdfMarshalling
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import cats.effect.IO
import cats.implicits.catsSyntaxApplicativeError
import io.circe.{Json, JsonObject}
import kamon.instrumentation.akka.http.TracingDirectives.operationName

class SearchRoutes(
    identities: Identities,
    aclCheck: AclCheck,
    search: Search,
    configFields: Json,
    suites: SearchConfig.Suites
)(implicit baseUri: BaseUri, cr: RemoteContextResolution, ordering: JsonKeyOrdering)
    extends AuthDirectives(identities, aclCheck)
    with CirceUnmarshalling
    with RdfMarshalling
    with DeltaDirectives {

  import baseUri.prefixSegment

  private val addProjectParam = "addProject"

  private def additionalProjects = parameter(addProjectParam.as[ProjectRef].*)

  private val searchExceptionHandler = ExceptionHandler { case err: SearchRejection =>
    discardEntityAndForceEmit(err)
  }.withFallback(ElasticSearchExceptionHandler.client)

  def routes: Route =
    baseUriPrefix(baseUri.prefix) {
      handleExceptions(searchExceptionHandler) {
        pathPrefix("search") {
          extractCaller { implicit caller =>
            concat(
              // Query the underlying aggregate elasticsearch view for global search
              (pathPrefix("query") & post) {
                (extractQueryParams & entity(as[JsonObject])) { (qp, payload) =>
                  concat(
                    pathEndOrSingleSlash {
                      emit(search.query(payload, qp).attemptNarrow[SearchRejection])
                    },
                    (pathPrefix("suite") & label & additionalProjects & pathEndOrSingleSlash) {
                      (suite, additionalProjects) =>
                        val filteredQp = qp.filterNot { case (key, _) => key == addProjectParam }
                        emit(
                          search
                            .query(suite, additionalProjects.toSet, payload, filteredQp)
                            .attemptNarrow[SearchRejection]
                        )
                    }
                  )
                }
              },
              // Get fields config
              (pathPrefix("config") & get & pathEndOrSingleSlash) {
                operationName(s"$prefixSegment/search/config") {
                  emit(IO.pure(configFields))
                }
              },
              // Fetch suite
              (pathPrefix("suites") & get & label & pathEndOrSingleSlash) { suiteName =>
                emit(
                  IO.fromOption(suites.get(suiteName))(UnknownSuite(suiteName))
                    .map(s => NamedSuite(suiteName, s))
                    .attemptNarrow[SearchRejection]
                )
              }
            )
          }
        }
      }
    }
}
