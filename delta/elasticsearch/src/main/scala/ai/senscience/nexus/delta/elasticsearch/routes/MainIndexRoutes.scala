package ai.senscience.nexus.delta.elasticsearch.routes

import ai.senscience.nexus.akka.marshalling.CirceUnmarshalling
import ai.senscience.nexus.delta.elasticsearch.indexing.mainIndexingProjection
import ai.senscience.nexus.delta.elasticsearch.model.permissions.{read as Read, write as Write}
import ai.senscience.nexus.delta.elasticsearch.model.{defaultViewId, permissions}
import ai.senscience.nexus.delta.elasticsearch.query.MainIndexQuery
import ai.senscience.nexus.delta.elasticsearch.routes.ElasticSearchViewsDirectives.extractQueryParams
import ai.senscience.nexus.delta.rdf.Vocabulary
import ai.senscience.nexus.delta.rdf.jsonld.context.JsonLdContext.keywords
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.directives.AuthDirectives
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.*
import ai.senscience.nexus.delta.sdk.error.ServiceError.ResourceNotFound
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sdk.marshalling.RdfMarshalling
import ai.senscience.nexus.delta.sdk.model.IdSegment
import ai.senscience.nexus.delta.sourcing.ProgressStatistics
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.projections.Projections
import ai.senscience.nexus.delta.sourcing.query.SelectFilter
import akka.http.scaladsl.server.{Directive, Route}
import io.circe.generic.semiauto.deriveEncoder
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, JsonObject}

final class MainIndexRoutes(
    identities: Identities,
    aclCheck: AclCheck,
    defaultIndexQuery: MainIndexQuery,
    projections: Projections
)(implicit cr: RemoteContextResolution, ordering: JsonKeyOrdering)
    extends AuthDirectives(identities, aclCheck)
    with CirceUnmarshalling
    with RdfMarshalling {

  implicit private val viewStatisticEncoder: Encoder.AsObject[ProgressStatistics] =
    deriveEncoder[ProgressStatistics].mapJsonObject(_.add(keywords.tpe, "ViewStatistics".asJson))

  implicit private val viewStatisticJsonLdEncoder: JsonLdEncoder[ProgressStatistics] =
    JsonLdEncoder.computeFromCirce(ContextValue(Vocabulary.contexts.statistics))

  private def defaultViewSegment: Directive[Unit] =
    idSegment.flatMap {
      case IdSegment.StringSegment(string) if string == "documents" => tprovide(())
      case IdSegment.IriSegment(iri) if iri == defaultViewId        => tprovide(())
      case _                                                        => reject()
    }

  def routes: Route =
    handleExceptions(ElasticSearchExceptionHandler.apply) {
      pathPrefix("views") {
        extractCaller { implicit caller =>
          projectRef { project =>
            val authorizeRead = authorizeFor(project, Read)
            val projection    = mainIndexingProjection(project)
            defaultViewSegment {
              concat(
                // Fetch statistics for the main indexing on this current project
                (pathPrefix("statistics") & get & pathEndOrSingleSlash & authorizeRead) {
                  emit(projections.statistics(project, SelectFilter.latest, projection))
                },
                // Manage an elasticsearch view offset
                (pathPrefix("offset") & pathEndOrSingleSlash) {
                  concat(
                    // Fetch an elasticsearch view offset
                    (get & authorizeFor(project, Read)) {
                      emit(projections.offset(projection))
                    },
                    // Remove an elasticsearch view offset (restart the view)
                    (delete & authorizeFor(project, Write)) {
                      emit(projections.offset(projection).as(Offset.start))
                    }
                  )
                },
                // Getting indexing status for a resource in the given view
                (pathPrefix("status") & authorizeRead & iriSegment & pathEndOrSingleSlash) { resourceId =>
                  emit(
                    projections
                      .indexingStatus(project, SelectFilter.latest, projection, resourceId)(
                        ResourceNotFound(resourceId, project)
                      )
                      .map(_.asJson)
                  )
                },
                // Query default indexing for this given project
                (pathPrefix("_search") & post & pathEndOrSingleSlash) {
                  authorizeFor(project, permissions.query).apply {
                    (extractQueryParams & entity(as[JsonObject])) { (qp, query) =>
                      emit(defaultIndexQuery.search(project, query, qp))
                    }
                  }
                }
              )
            }
          }
        }
      }
    }
}
