package ai.senscience.nexus.delta.routes

import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.routes.SupervisionRoutes.{allProjectsAreHealthy, healingSuccessfulResponse, unhealthyProjectsEncoder, SupervisionBundle}
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress
import ai.senscience.nexus.delta.sdk.directives.*
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.*
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.marshalling.RdfMarshalling
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.permissions.Permissions.{projects, supervision}
import ai.senscience.nexus.delta.sdk.projects.{ProjectHealer, ProjectsHealth}
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.stream.SupervisedDescription
import cats.effect.IO
import cats.effect.unsafe.implicits.*
import io.circe.generic.semiauto.deriveEncoder
import io.circe.syntax.KeyOps
import io.circe.{Encoder, Json}
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.server.*
import org.typelevel.otel4s.trace.Tracer

class SupervisionRoutes(
    identities: Identities,
    aclCheck: AclCheck,
    supervised: IO[List[SupervisedDescription]],
    projectsHealth: ProjectsHealth,
    projectHealer: ProjectHealer
)(using baseUri: BaseUri)(using JsonKeyOrdering, Tracer[IO])
    extends AuthDirectives(identities, aclCheck)
    with RdfMarshalling {

  def routes: Route =
    baseUriPrefix(baseUri.prefix) {
      pathPrefix("supervision") {
        extractCaller { case given Caller =>
          concat(
            authorizeFor(AclAddress.Root, supervision.read).apply {
              concat(
                (pathPrefix("projections") & get & pathEndOrSingleSlash) {
                  emitJson(supervised.map(SupervisionBundle(_)))
                },
                (pathPrefix("projects") & get & pathEndOrSingleSlash) {
                  onSuccess(projectsHealth.health.unsafeToFuture()) { projects =>
                    if projects.isEmpty then complete(StatusCodes.OK, allProjectsAreHealthy)
                    else complete(StatusCodes.InternalServerError, unhealthyProjectsEncoder(projects))
                  }
                }
              )
            },
            authorizeFor(AclAddress.Root, projects.write).apply {
              (post & pathPrefix("projects") & projectRef & pathPrefix("heal") & pathEndOrSingleSlash) { project =>
                emit(
                  projectHealer
                    .heal(project)
                    .map(_ => healingSuccessfulResponse(project))
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

  given Encoder[SupervisionBundle] = deriveEncoder

  private val allProjectsAreHealthy =
    Json.obj("status" := "All projects are healthy.")

  private val unhealthyProjectsEncoder: Encoder[Set[ProjectRef]] =
    Encoder.instance { set =>
      Json.obj("status" := "Some projects are unhealthy.", "unhealthyProjects" := set)
    }

  private def healingSuccessfulResponse(project: ProjectRef) =
    Json.obj("message" := s"Project '$project' has been healed.")

}
