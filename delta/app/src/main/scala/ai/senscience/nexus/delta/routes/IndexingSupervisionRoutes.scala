package ai.senscience.nexus.delta.routes

import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.routes.IndexingSupervisionRoutes.IndexingErrorBundle
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress
import ai.senscience.nexus.delta.sdk.directives.AuthDirectives
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.{baseUriPrefix, emitJson, timeRange}
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.indexing.*
import ai.senscience.nexus.delta.sdk.marshalling.RdfMarshalling
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.permissions.Permissions.supervision
import ai.senscience.nexus.delta.sourcing.model.FailedElemLog
import ai.senscience.nexus.delta.sourcing.projections.ProjectionErrors
import akka.http.scaladsl.server.Route
import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder

final class IndexingSupervisionRoutes(
    identities: Identities,
    aclCheck: AclCheck,
    projectionErrors: ProjectionErrors
)(implicit
    baseUri: BaseUri,
    ordering: JsonKeyOrdering
) extends AuthDirectives(identities, aclCheck)
    with RdfMarshalling {

  def routes: Route =
    baseUriPrefix(baseUri.prefix) {
      (pathPrefix("supervision") & pathPrefix("indexing")) {
        extractCaller { implicit caller =>
          pathPrefix("errors") {
            authorizeFor(AclAddress.Root, supervision.read).apply {
              concat(
                (pathPrefix("count") & timeRange("instant") & get & pathEndOrSingleSlash) { timeRange =>
                  emitJson(projectionErrors.count(timeRange))
                },
                (pathPrefix("latest") & parameter("rev".as[Int].?) & get & pathEndOrSingleSlash) { sizeOpt =>
                  emitJson(projectionErrors.latest(sizeOpt.getOrElse(100)).map(IndexingErrorBundle(_)))
                }
              )
            }
          }
        }
      }
    }

}

object IndexingSupervisionRoutes {

  private case class IndexingErrorBundle(errors: List[FailedElemLog])

  private object IndexingErrorBundle {
    implicit val indexingErrorBundleEncoder: Encoder[IndexingErrorBundle] = deriveEncoder[IndexingErrorBundle]
  }

}
