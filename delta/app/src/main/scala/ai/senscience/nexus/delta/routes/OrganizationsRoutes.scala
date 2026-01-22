package ai.senscience.nexus.delta.routes

import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.routes.OrganizationsRoutes.OrganizationInput
import ai.senscience.nexus.delta.sdk.OrganizationResource
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.directives.AuthDirectives
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.*
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.model.search.SearchParams.OrganizationSearchParams
import ai.senscience.nexus.delta.sdk.model.search.SearchResults.*
import ai.senscience.nexus.delta.sdk.model.search.{PaginationConfig, SearchResults}
import ai.senscience.nexus.delta.sdk.organizations.model.OrganizationRejection.*
import ai.senscience.nexus.delta.sdk.organizations.model.{Organization, OrganizationRejection}
import ai.senscience.nexus.delta.sdk.organizations.{OrganizationDeleter, Organizations}
import ai.senscience.nexus.delta.sdk.permissions.Permissions.*
import ai.senscience.nexus.pekko.marshalling.CirceUnmarshalling
import cats.effect.IO
import cats.syntax.all.*
import io.circe.Decoder
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveConfiguredDecoder
import org.apache.pekko.http.scaladsl.model.{StatusCode, StatusCodes}
import org.apache.pekko.http.scaladsl.server.{Directive1, Route}
import org.typelevel.otel4s.trace.Tracer

/**
  * The organization routes.
  *
  * @param identities
  *   the identities operations bundle
  * @param aclCheck
  *   verify the acl for users
  * @param organizations
  *   the organizations operations bundle
  */
final class OrganizationsRoutes(
    identities: Identities,
    aclCheck: AclCheck,
    organizations: Organizations,
    orgDeleter: OrganizationDeleter
)(using baseUri: BaseUri)(using PaginationConfig, RemoteContextResolution, JsonKeyOrdering, Tracer[IO])
    extends AuthDirectives(identities, aclCheck)
    with CirceUnmarshalling {

  private def orgsSearchParams(using Caller): Directive1[OrganizationSearchParams] =
    (searchParams & parameter("label".?)).tmap { case (deprecated, rev, createdBy, updatedBy, label) =>
      OrganizationSearchParams(
        deprecated,
        rev,
        createdBy,
        updatedBy,
        label,
        org => aclCheck.authorizeFor(org.label, orgs.read)
      )
    }

  private def emitMetadata(statusCode: StatusCode, io: IO[OrganizationResource]): Route =
    emit(statusCode, io.mapValue(_.metadata))

  private def emitMetadata(io: IO[OrganizationResource]): Route =
    emitMetadata(StatusCodes.OK, io)

  private def orgDescription = entity(as[OrganizationInput]).map(_.description)

  def routes: Route =
    baseUriPrefix(baseUri.prefix) {
      pathPrefix("orgs") {
        extractCaller { case given Caller =>
          concat(
            // List organizations
            (get & extractHttp4sUri & fromPaginated & orgsSearchParams & sort[Organization] & pathEndOrSingleSlash) {
              (uri, pagination, params, order) =>
                given JsonLdEncoder[SearchResults[OrganizationResource]] =
                  searchResultsJsonLdEncoder(Organization.context, pagination, uri)
                emit(organizations.list(pagination, params, order).widen[SearchResults[OrganizationResource]])
            },
            (label & pass) { org =>
              val authorizeCreate = authorizeFor(org, orgs.create)
              val authorizeRead   = authorizeFor(org, orgs.read)
              val authorizeWrite  = authorizeFor(org, orgs.write)
              val authorizeDelete = authorizeFor(org, orgs.delete)
              concat(
                pathEndOrSingleSlash {
                  concat(
                    (put & noRev & authorizeCreate) {
                      // Create organization
                      orgDescription { description =>
                        emitMetadata(StatusCodes.Created, organizations.create(org, description))
                      }
                    },
                    (put & authorizeWrite & revParam) { rev =>
                      // Update organization
                      orgDescription { description =>
                        emitMetadata(organizations.update(org, description, rev))
                      }
                    },
                    (get & authorizeRead & revParamOpt) {
                      case Some(rev) => // Fetch organization at specific revision
                        emit(organizations.fetchAt(org, rev))
                      case None      => // Fetch organization
                        emit(organizations.fetch(org))
                    },
                    // Deprecate or delete an organization
                    (delete & revParamOpt & parameter("prune".as[Boolean].?)) {
                      case (Some(rev), prune) if !prune.contains(true) =>
                        authorizeWrite { emitMetadata(organizations.deprecate(org, rev)) }
                      case (None, Some(true))                          =>
                        authorizeDelete { emit(orgDeleter(org)) }
                      case (_, _)                                      => discardEntityAndForceEmit(InvalidDeleteRequest(org): OrganizationRejection)
                    }
                  )
                },
                (put & pathPrefix("undeprecate") & pathEndOrSingleSlash & revParam) { rev =>
                  authorizeWrite {
                    emitMetadata(organizations.undeprecate(org, rev))
                  }
                }
              )
            }
          )
        }
      }
    }
}

object OrganizationsRoutes {
  final private[routes] case class OrganizationInput(description: Option[String])

  private[routes] object OrganizationInput {
    private given Configuration      = Configuration.default.withStrictDecoding
    given Decoder[OrganizationInput] = deriveConfiguredDecoder[OrganizationInput]
  }

  /**
    * @return
    *   the [[Route]] for organizations
    */
  def apply(
      identities: Identities,
      aclCheck: AclCheck,
      organizations: Organizations,
      orgDeleter: OrganizationDeleter
  )(using BaseUri, PaginationConfig, RemoteContextResolution, JsonKeyOrdering, Tracer[IO]): Route =
    new OrganizationsRoutes(identities, aclCheck, organizations, orgDeleter).routes

}
