package ai.senscience.nexus.delta.elasticsearch.routes

import ai.senscience.nexus.delta.elasticsearch.IdResolution
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.directives.AuthDirectives
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.*
import ai.senscience.nexus.delta.sdk.directives.OtelDirectives.routeSpan
import ai.senscience.nexus.delta.sdk.fusion.FusionConfig
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.model.BaseUri
import cats.effect.IO
import org.apache.pekko.http.scaladsl.model.{StatusCodes, Uri}
import org.apache.pekko.http.scaladsl.server.Route
import org.typelevel.otel4s.trace.Tracer

class IdResolutionRoutes(
    identities: Identities,
    aclCheck: AclCheck,
    idResolution: IdResolution
)(using baseUri: BaseUri, fusionConfig: FusionConfig)(using JsonKeyOrdering, RemoteContextResolution, Tracer[IO])
    extends AuthDirectives(identities, aclCheck) {

  def routes: Route =
    handleExceptions(ElasticSearchExceptionHandler.apply) {
      concat(resolutionRoute, proxyRoute)
    }

  private def resolutionRoute: Route =
    routeSpan("views/<str:org>/<str:project>/documents/_search") {
      pathPrefix("resolve") {
        extractCaller { case given Caller =>
          (get & iriSegment & pathEndOrSingleSlash) { iri =>
            emit(idResolution.apply(iri))
          }
        }
      }
    }

  private def proxyRoute: Route =
    pathPrefix("resolve-proxy-pass") {
      extractUnmatchedPath { path =>
        get {
          val htt4sPath  = org.http4s.Uri.unsafeFromString(path.toString())
          val resourceId = fusionConfig.resolveBase.resolve(htt4sPath)
          emitOrFusionRedirect(
            fusionResolveUri(resourceId),
            redirect(deltaResolveEndpoint(resourceId), StatusCodes.SeeOther)
          )
        }
      }
    }

  private def deltaResolveEndpoint(id: org.http4s.Uri): Uri =
    Uri((baseUri.endpoint / "resolve" / id.toString).toString())

}
