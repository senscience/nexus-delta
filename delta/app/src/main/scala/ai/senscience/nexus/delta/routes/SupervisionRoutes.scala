package ai.senscience.nexus.delta.routes

import ai.senscience.nexus.delta.rdf.Vocabulary.contexts
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.routes.SupervisionRoutes.{SupervisionBundle, allProjectsAreHealthy, healingSuccessfulResponse, unhealthyProjectsEncoder}
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress
import ai.senscience.nexus.delta.sdk.directives.*
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.emit
import ai.senscience.nexus.delta.sdk.directives.UriDirectives.{baseUriPrefix, projectRef}
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.marshalling.{HttpResponseFields, RdfMarshalling}
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.permissions.Permissions.{projects, supervision}
import ai.senscience.nexus.delta.sdk.projects.model.ProjectRejection
import ai.senscience.nexus.delta.sdk.projects.{ProjectHealer, ProjectsHealth}
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.stream.{ProjectActivitySignals, SupervisedDescription}
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.*
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.catsSyntaxApplicativeError
import io.circe.generic.semiauto.deriveEncoder
import io.circe.syntax.{EncoderOps, KeyOps}
import io.circe.{Encoder, Json}

class SupervisionRoutes(
    identities: Identities,
    aclCheck: AclCheck,
    supervised: IO[List[SupervisedDescription]],
    projectsHealth: ProjectsHealth,
    projectHealer: ProjectHealer,
    activitySignals: ProjectActivitySignals
)(implicit
    baseUri: BaseUri,
    cr: RemoteContextResolution,
    ordering: JsonKeyOrdering
) extends AuthDirectives(identities, aclCheck)
    with RdfMarshalling {

  def routes: Route =
    baseUriPrefix(baseUri.prefix) {
      pathPrefix("supervision") {
        extractCaller { implicit caller =>
          concat(
            authorizeFor(AclAddress.Root, supervision.read).apply {
              concat(
                (pathPrefix("projections") & get & pathEndOrSingleSlash) {
                  emit(supervised.map(SupervisionBundle))
                },
                (pathPrefix("projects") & get & pathEndOrSingleSlash) {
                  onSuccess(projectsHealth.health.unsafeToFuture()) { projects =>
                    if (projects.isEmpty) emit(StatusCodes.OK, IO.pure(allProjectsAreHealthy))
                    else emit(StatusCodes.InternalServerError, IO.pure(unhealthyProjectsEncoder(projects)))
                  }
                },
                (pathPrefix("activity") & pathPrefix("projects") & get & pathEndOrSingleSlash) {
                  emit(activitySignals.activityMap.map(_.asJson))
                }
              )
            },
            authorizeFor(AclAddress.Root, projects.write).apply {
              (post & pathPrefix("projects") & projectRef & pathPrefix("heal") & pathEndOrSingleSlash) { project =>
                emit(
                  projectHealer
                    .heal(project)
                    .map(_ => healingSuccessfulResponse(project))
                    .attemptNarrow[ProjectRejection]
                )
              }
            }
          )
        }
      }
    }

}

object SupervisionRoutes {

  case class SupervisionBundle(projections: List[SupervisedDescription])

  implicit final val runningProjectionsEncoder: Encoder[SupervisionBundle]       =
    deriveEncoder
  implicit val runningProjectionsJsonLdEncoder: JsonLdEncoder[SupervisionBundle] =
    JsonLdEncoder.computeFromCirce(ContextValue(contexts.supervision))

  implicit val versionHttpResponseFields: HttpResponseFields[SupervisionBundle] = HttpResponseFields.defaultOk

  private val allProjectsAreHealthy =
    Json.obj("status" := "All projects are healthy.")

  private val unhealthyProjectsEncoder: Encoder[Set[ProjectRef]] =
    Encoder.instance { set =>
      Json.obj("status" := "Some projects are unhealthy.", "unhealthyProjects" := set)
    }

  private def healingSuccessfulResponse(project: ProjectRef) =
    Json.obj("message" := s"Project '$project' has been healed.")

}
