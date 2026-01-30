package ai.senscience.nexus.delta.routes

import ai.senscience.nexus.delta.rdf.jsonld.context.JsonLdContext.keywords
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.routes.AclsRoutes.PatchAcl.{Append, Subtract}
import ai.senscience.nexus.delta.routes.AclsRoutes.{PatchAcl, ReplaceAcl}
import ai.senscience.nexus.delta.sdk.AclResource
import ai.senscience.nexus.delta.sdk.acls.model.*
import ai.senscience.nexus.delta.sdk.acls.model.AclAddressFilter.{AnyOrganization, AnyOrganizationAnyProject, AnyProject}
import ai.senscience.nexus.delta.sdk.acls.model.AclRejection.AclNotFound
import ai.senscience.nexus.delta.sdk.acls.{AclCheck, Acls}
import ai.senscience.nexus.delta.sdk.directives.AuthDirectives
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.*
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sdk.marshalling.{QueryParamsUnmarshalling, RdfRejectionHandler}
import ai.senscience.nexus.delta.sdk.marshalling.RdfRejectionHandler.given
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.model.search.SearchResults
import ai.senscience.nexus.delta.sdk.model.search.SearchResults.searchResultsJsonLdEncoder
import ai.senscience.nexus.delta.sdk.permissions.Permissions.acls as aclsPermissions
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.{Label, ProjectRef}
import ai.senscience.nexus.pekko.marshalling.CirceUnmarshalling
import cats.effect.IO
import io.circe.*
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveConfiguredDecoder
import org.apache.pekko.http.javadsl.server.Rejections.validationRejection
import org.apache.pekko.http.scaladsl.model.StatusCodes.*
import org.apache.pekko.http.scaladsl.model.Uri.Path
import org.apache.pekko.http.scaladsl.model.Uri.Path.*
import org.apache.pekko.http.scaladsl.model.{StatusCode, StatusCodes}
import org.apache.pekko.http.scaladsl.server.{Directive1, ExceptionHandler, MalformedQueryParamRejection, Route}
import org.typelevel.otel4s.trace.Tracer

class AclsRoutes(identities: Identities, aclCheck: AclCheck, acls: Acls)(using baseUri: BaseUri)(using
    RemoteContextResolution,
    JsonKeyOrdering,
    Tracer[IO]
) extends AuthDirectives(identities, aclCheck)
    with CirceUnmarshalling
    with QueryParamsUnmarshalling {

  private val any = "*"

  private val simultaneousRevAndAncestorsRejection =
    MalformedQueryParamRejection("rev", "rev and ancestors query parameters cannot be present simultaneously.")

  private given JsonLdEncoder[SearchResults[AclResource]] = searchResultsJsonLdEncoder(Acl.context)

  private given JsonLdEncoder[MalformedQueryParamRejection] =
    RdfRejectionHandler.compactFromCirceRejection[MalformedQueryParamRejection]

  private val exceptionHandler = ExceptionHandler { case err: AclRejection =>
    discardEntityAndForceEmit(err)
  }

  private def extractAclAddress: Directive1[AclAddress] =
    extractUnmatchedPath.flatMap {
      case SingleSlash                                                                                            => provide(AclAddress.Root)
      case Path.Empty                                                                                             => provide(AclAddress.Root)
      case Path.Slash(Path.Segment(org, Path.Empty)) if org != any                                                => label(org).map(AclAddress.fromOrg)
      case Path.Slash(Path.Segment(org, Path.Slash(Path.Segment(proj, Path.Empty)))) if org != any && proj != any =>
        for {
          orgLabel  <- label(org)
          projLabel <- label(proj)
        } yield AclAddress.fromProject(ProjectRef(orgLabel, projLabel))
      case _                                                                                                      => reject

    }

  private def extractAclAddressFilter: Directive1[AclAddressFilter] =
    (extractUnmatchedPath & parameter("ancestors" ? false)).tflatMap { case (path, ancestors) =>
      path match {
        case Path.Slash(Path.Segment(`any`, Path.Empty))                                  => provide(AnyOrganization(ancestors))
        case Path.Slash(Path.Segment(`any`, Path.Slash(Path.Segment(`any`, Path.Empty)))) =>
          provide(AnyOrganizationAnyProject(ancestors))
        case Path.Slash(Path.Segment(org, Path.Slash(Path.Segment(`any`, Path.Empty))))   =>
          Label(org).fold(
            err => reject(validationRejection(err.getMessage)),
            label => provide[AclAddressFilter](AnyProject(label, ancestors))
          )
        case _                                                                            => reject
      }
    }

  private def emitMetadata(statusCode: StatusCode, io: IO[AclResource]): Route =
    emit(statusCode, io.mapValue(_.metadata))

  private def emitMetadata(io: IO[AclResource]): Route = emitMetadata(StatusCodes.OK, io)

  private def emitWithoutAncestors(io: IO[AclResource]): Route = emit {
    io.map(Option(_))
      .recover { case AclNotFound(_) => None }
      .map(searchResults(_))
  }

  private def emitWithAncestors(io: IO[AclCollection]) =
    emit(io.map { collection => searchResults(collection.value.values) })

  private def searchResults(iter: Iterable[AclResource]): SearchResults[AclResource] = {
    val vector = iter.toVector
    SearchResults(vector.length.toLong, vector)
  }

  private val revParamOrZero = parameter("rev" ? 0)
  private val selfParam      = parameter("self" ? true)
  private val ancestorsParam = parameter("ancestors" ? false)

  def routes: Route = {
    (baseUriPrefix(baseUri.prefix) & handleExceptions(exceptionHandler)) {
      pathPrefix("acls") {
        extractCaller { case caller @ given Caller =>
          given Subject = caller.subject
          concat(
            extractAclAddress { address =>
              val authorizeRead  = authorizeFor(address, aclsPermissions.read)
              val authorizeWrite = authorizeFor(address, aclsPermissions.write)
              revParamOrZero { rev =>
                concat(
                  // Replace ACLs
                  (put & authorizeWrite & entity(as[ReplaceAcl])) { case ReplaceAcl(AclValues(values)) =>
                    val status = if rev == 0 then Created else OK
                    emitMetadata(status, acls.replace(Acl(address, values*), rev))
                  },
                  // Append or subtract ACLs
                  (patch & entity(as[PatchAcl]) & authorizeWrite) {
                    case Append(AclValues(values))   =>
                      emitMetadata(acls.append(Acl(address, values*), rev))
                    case Subtract(AclValues(values)) =>
                      emitMetadata(acls.subtract(Acl(address, values*), rev))
                  },
                  // Delete ACLs
                  (delete & authorizeWrite) {
                    emitMetadata(acls.delete(address, rev))
                  },
                  // Fetch ACLs
                  (get & selfParam) {
                    case true  =>
                      (revParamOpt & ancestorsParam) {
                        case (Some(_), true)    => emit(simultaneousRevAndAncestorsRejection)
                        case (Some(rev), false) =>
                          // Fetch self ACLs without ancestors at specific revision
                          emitWithoutAncestors(acls.fetchSelfAt(address, rev))
                        case (None, true)       =>
                          // Fetch self ACLs with ancestors
                          emitWithAncestors(acls.fetchSelfWithAncestors(address))
                        case (None, false)      =>
                          // Fetch self ACLs without ancestors
                          emitWithoutAncestors(acls.fetchSelf(address))
                      }
                    case false =>
                      (authorizeRead & revParamOpt & ancestorsParam) {
                        case (Some(_), true)    => reject(simultaneousRevAndAncestorsRejection)
                        case (Some(rev), false) =>
                          // Fetch all ACLs without ancestors at specific revision
                          emitWithoutAncestors(acls.fetchAt(address, rev))
                        case (None, true)       =>
                          // Fetch all ACLs with ancestors
                          emitWithAncestors(acls.fetchWithAncestors(address))
                        case (None, false)      =>
                          // Fetch all ACLs without ancestors
                          emitWithoutAncestors(acls.fetch(address))
                      }
                  }
                )
              }
            },
            // Filter ACLs
            (get & extractAclAddressFilter & selfParam) { case (addressFilter, self) =>
              emitWithAncestors(fetchFilteredAcls(addressFilter, self))
            }
          )
        }
      }
    }
  }

  private def fetchFilteredAcls(addressFilter: AclAddressFilter, self: Boolean)(using caller: Caller) = {
    if self then {
      acls.listSelf(addressFilter).map(_.removeEmpty())
    } else {
      acls
        .list(addressFilter)
        .map { aclCol =>
          val accessibleAcls = aclCol.filterByPermission(caller.identities, aclsPermissions.read)
          val callerAcls     = aclCol.filter(caller.identities)
          accessibleAcls ++ callerAcls
        }
    }
  }
}

object AclsRoutes {

  final private[routes] case class ReplaceAcl(acl: AclValues)
  private[routes] object ReplaceAcl {

    given Decoder[ReplaceAcl] = {
      given Configuration = Configuration.default.withStrictDecoding
      deriveConfiguredDecoder[ReplaceAcl]
    }
  }

  sealed private[routes] trait PatchAcl extends Product with Serializable
  private[routes] object PatchAcl {
    final case class Subtract(acl: AclValues) extends PatchAcl
    final case class Append(acl: AclValues)   extends PatchAcl

    given Decoder[PatchAcl] = {
      given Configuration = Configuration.default.withStrictDecoding.withDiscriminator(keywords.tpe)
      deriveConfiguredDecoder[PatchAcl]
    }
  }

  /**
    * @return
    *   the [[Route]] for ACLs
    */
  def apply(identities: Identities, aclCheck: AclCheck, acls: Acls)(using
      BaseUri,
      RemoteContextResolution,
      JsonKeyOrdering,
      Tracer[IO]
  ): AclsRoutes =
    new AclsRoutes(identities, aclCheck, acls)

}
