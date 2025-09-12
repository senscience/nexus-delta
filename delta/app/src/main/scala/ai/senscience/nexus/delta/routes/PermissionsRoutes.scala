package ai.senscience.nexus.delta.routes

import ai.senscience.nexus.delta.rdf.Vocabulary.contexts
import ai.senscience.nexus.delta.rdf.jsonld.context.JsonLdContext.keywords
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.routes.PermissionsRoutes.PatchPermissions
import ai.senscience.nexus.delta.routes.PermissionsRoutes.PatchPermissions.{Append, Replace, Subtract}
import ai.senscience.nexus.delta.sdk.PermissionsResource
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress
import ai.senscience.nexus.delta.sdk.directives.AuthDirectives
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.*
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sdk.model.{BaseUri, ResourceF}
import ai.senscience.nexus.delta.sdk.permissions.Permissions
import ai.senscience.nexus.delta.sdk.permissions.Permissions.permissions as permissionsPerms
import ai.senscience.nexus.delta.sdk.permissions.model.{Permission, PermissionsRejection}
import ai.senscience.nexus.pekko.marshalling.CirceUnmarshalling
import cats.effect.IO
import cats.syntax.all.*
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveConfiguredDecoder
import io.circe.syntax.*
import io.circe.{Decoder, Json}
import org.apache.pekko.http.scaladsl.server.{ExceptionHandler, MalformedRequestContentRejection, Route}

/**
  * The permissions routes.
  *
  * @param identities
  *   the identities operations bundle
  * @param permissions
  *   the permissions operations bundle
  * @param aclCheck
  *   verify the acls for users
  */
final class PermissionsRoutes(identities: Identities, permissions: Permissions, aclCheck: AclCheck)(implicit
    baseUri: BaseUri,
    cr: RemoteContextResolution,
    ordering: JsonKeyOrdering
) extends AuthDirectives(identities, aclCheck)
    with CirceUnmarshalling {

  implicit private val resourceFUnitJsonLdEncoder: JsonLdEncoder[ResourceF[Unit]] =
    ResourceF.resourceFAJsonLdEncoder(ContextValue(contexts.permissionsMetadata))

  private val exceptionHandler = ExceptionHandler { case err: PermissionsRejection =>
    discardEntityAndForceEmit(err)
  }

  private def authorizeRead(implicit caller: Caller)  = authorizeFor(AclAddress.Root, permissionsPerms.read)
  private def authorizeWrite(implicit caller: Caller) = authorizeFor(AclAddress.Root, permissionsPerms.write)

  private def emitMetadata(io: IO[PermissionsResource]): Route = emit(io.map(_.void))

  def routes: Route =
    (baseUriPrefix(baseUri.prefix) & handleExceptions(exceptionHandler)) {
      pathPrefix("permissions") {
        extractCaller { implicit caller =>
          concat(
            pathEndOrSingleSlash {
              concat(
                // Fetch permissions
                get {
                  authorizeRead.apply {
                    parameter("rev".as[Int].?) {
                      case Some(rev) => emit(permissions.fetchAt(rev))
                      case None      => emit(permissions.fetch)
                    }
                  }
                },
                // Replace permissions
                (put & parameter("rev" ? 0)) { rev =>
                  authorizeWrite.apply {
                    entity(as[PatchPermissions]) {
                      case Replace(set) => emitMetadata(permissions.replace(set, rev))
                      case _            =>
                        malformedContent(s"Value for field '${keywords.tpe}' must be 'Replace' when using 'PUT'.")
                    }
                  }
                },
                // Append or Subtract permissions
                (patch & parameter("rev" ? 0)) { rev =>
                  authorizeWrite.apply {
                    entity(as[PatchPermissions]) {
                      case Append(set)   => emitMetadata(permissions.append(set, rev))
                      case Subtract(set) => emitMetadata(permissions.subtract(set, rev))
                      case _             =>
                        malformedContent(
                          s"Value for field '${keywords.tpe}' must be 'Append' or 'Subtract' when using 'PATCH'."
                        )
                    }
                  }
                },
                // Delete permissions
                delete {
                  authorizeWrite.apply {
                    parameter("rev".as[Int]) { rev =>
                      emitMetadata(permissions.delete(rev))
                    }
                  }
                }
              )
            }
          )
        }
      }
    }

  private def malformedContent(field: String) =
    reject(MalformedRequestContentRejection(field, new IllegalArgumentException()))
}

object PermissionsRoutes {

  /**
    * @return
    *   the [[Route]] for the permission resources
    */
  def apply(identities: Identities, permissions: Permissions, aclCheck: AclCheck)(implicit
      baseUri: BaseUri,
      cr: RemoteContextResolution,
      ordering: JsonKeyOrdering
  ): Route =
    new PermissionsRoutes(identities, permissions, aclCheck: AclCheck).routes

  sealed private[routes] trait PatchPermissions extends Product with Serializable

  private[routes] object PatchPermissions {

    final case class Append(permissions: Set[Permission])   extends PatchPermissions
    final case class Subtract(permissions: Set[Permission]) extends PatchPermissions
    final case class Replace(permissions: Set[Permission])  extends PatchPermissions

    implicit final private val configuration: Configuration =
      Configuration.default.withStrictDecoding.withDiscriminator(keywords.tpe)

    private val replacedType = Json.obj(keywords.tpe -> "Replace".asJson)

    implicit val patchPermissionsDecoder: Decoder[PatchPermissions] =
      Decoder.instance { hc =>
        deriveConfiguredDecoder[PatchPermissions].decodeJson(replacedType deepMerge hc.value)
      }
  }

}
