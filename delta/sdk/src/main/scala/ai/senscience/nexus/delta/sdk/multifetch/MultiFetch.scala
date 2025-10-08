package ai.senscience.nexus.delta.sdk.multifetch

import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.jsonld.JsonLdContent
import ai.senscience.nexus.delta.sdk.multifetch.model.MultiFetchResponse.Result.{AuthorizationFailed, NotFound, Success}
import ai.senscience.nexus.delta.sdk.multifetch.model.{MultiFetchRequest, MultiFetchResponse}
import ai.senscience.nexus.delta.sdk.permissions.Permissions.resources
import cats.effect.IO
import cats.syntax.all.*
import org.typelevel.otel4s.trace.Tracer

/**
  * Allows to fetch multiple resources of different types in one request.
  *
  * The response includes a resources array that contains the resources in the order specified in the request. If there
  * is a failure getting a particular resource, the error is included in place of the resource.
  */
trait MultiFetch {

  def apply(request: MultiFetchRequest)(implicit caller: Caller): IO[MultiFetchResponse]

}

object MultiFetch {

  def apply(
      aclCheck: AclCheck,
      fetchResource: MultiFetchRequest.Input => IO[Option[JsonLdContent[?]]]
  )(using Tracer[IO]): MultiFetch =
    new MultiFetch {
      override def apply(request: MultiFetchRequest)(implicit
          caller: Caller
      ): IO[MultiFetchResponse] =
        request.resources
          .parTraverse { input =>
            aclCheck.authorizeFor(input.project, resources.read).flatMap {
              case true  =>
                fetchResource(input).map {
                  _.map(Success(input.id, input.project, _))
                    .getOrElse(NotFound(input.id, input.project))
                }
              case false =>
                IO.pure(AuthorizationFailed(input.id, input.project))
            }
          }
          .map { resources =>
            MultiFetchResponse(request.format, resources)
          }
          .surround("multi-fetch")

    }
}
