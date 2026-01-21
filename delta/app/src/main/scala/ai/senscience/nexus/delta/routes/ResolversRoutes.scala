package ai.senscience.nexus.delta.routes

import ai.senscience.nexus.delta.rdf.Vocabulary.{contexts, schemas}
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.routes.ResolutionType.{AllResolversInProject, SingleResolver}
import ai.senscience.nexus.delta.sdk.*
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.*
import ai.senscience.nexus.delta.sdk.directives.Response.Reject
import ai.senscience.nexus.delta.sdk.directives.{AuthDirectives, DeltaSchemeDirectives}
import ai.senscience.nexus.delta.sdk.fusion.FusionConfig
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sdk.marshalling.{HttpResponseFields, RdfMarshalling}
import ai.senscience.nexus.delta.sdk.model.search.SearchResults
import ai.senscience.nexus.delta.sdk.model.search.SearchResults.searchResultsJsonLdEncoder
import ai.senscience.nexus.delta.sdk.model.source.OriginalSource
import ai.senscience.nexus.delta.sdk.model.{BaseUri, IdSegment, IdSegmentRef, ResourceF}
import ai.senscience.nexus.delta.sdk.permissions.Permissions
import ai.senscience.nexus.delta.sdk.permissions.Permissions.resolvers.{read as Read, write as Write}
import ai.senscience.nexus.delta.sdk.resolvers.model.ResolverRejection.ResolverNotFound
import ai.senscience.nexus.delta.sdk.resolvers.model.{MultiResolutionResult, Resolver, ResolverRejection}
import ai.senscience.nexus.delta.sdk.resolvers.{MultiResolution, Resolvers}
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.pekko.marshalling.CirceUnmarshalling
import cats.effect.IO
import cats.syntax.all.*
import org.apache.pekko.http.scaladsl.model.StatusCodes.Created
import org.apache.pekko.http.scaladsl.model.{StatusCode, StatusCodes}
import org.apache.pekko.http.scaladsl.server.*
import org.typelevel.otel4s.trace.Tracer

/**
  * The resolver routes
  *
  * @param identities
  *   the identity module
  * @param aclCheck
  *   verify the acls for users
  * @param resolvers
  *   the resolvers module
  * @param schemeDirectives
  *   directives related to orgs and projects
  */
final class ResolversRoutes(
    identities: Identities,
    aclCheck: AclCheck,
    resolvers: Resolvers,
    multiResolution: MultiResolution,
    schemeDirectives: DeltaSchemeDirectives
)(using baseUri: BaseUri)(using RemoteContextResolution, JsonKeyOrdering, FusionConfig, Tracer[IO])
    extends AuthDirectives(identities, aclCheck)
    with CirceUnmarshalling
    with RdfMarshalling {

  import schemeDirectives.*

  private def exceptionHandler(enableRejects: Boolean) =
    handleExceptions(
      ExceptionHandler { case err: ResolverRejection =>
        err match {
          case _: ResolverNotFound if enableRejects => reject(Reject(err))
          case _                                    => discardEntityAndForceEmit(err)
        }
      }
    )

  private given JsonLdEncoder[ResourceF[Unit]]                 =
    ResourceF.resourceFAJsonLdEncoder(ContextValue(contexts.resolversMetadata))
  private given JsonLdEncoder[SearchResults[ResolverResource]] = searchResultsJsonLdEncoder(Resolver.context)

  private def emitFetch(io: IO[ResolverResource]): Route =
    exceptionHandler(enableRejects = true) { emit(io) }

  private def emitMetadata(statusCode: StatusCode, io: IO[ResolverResource]): Route =
    exceptionHandler(enableRejects = false) {
      emit(statusCode, io.map(_.void))
    }

  private def emitMetadata(io: IO[ResolverResource]): Route = emitMetadata(StatusCodes.OK, io)

  private def emitMetadataOrReject(io: IO[ResolverResource]): Route =
    exceptionHandler(enableRejects = true) {
      emit(io.map(_.void))
    }

  private def emitSource(io: IO[ResolverResource]): Route =
    exceptionHandler(enableRejects = true) {
      emit(io.map { resource => OriginalSource(resource, resource.value.source) })
    }

  def routes: Route =
    (baseUriPrefix(baseUri.prefix) & replaceUri("resolvers", schemas.resolvers)) {
      pathPrefix("resolvers") {
        extractCaller { case given Caller =>
          projectRef { project =>
            val authorizeRead  = authorizeFor(project, Read)
            val authorizeWrite = authorizeFor(project, Write)
            concat(
              // List resolvers
              pathEndOrSingleSlash {
                (get & authorizeRead) {
                  emit(resolvers.list(project).widen[SearchResults[ResolverResource]])
                }
              },
              pathEndOrSingleSlash {
                // Create a resolver without an id segment
                (post & noRev & authorizeWrite & jsonEntity) { payload =>
                  emitMetadata(Created, resolvers.create(project, payload))
                }
              },
              idSegment { resolver =>
                concat(
                  pathEndOrSingleSlash {
                    concat(
                      (put & authorizeWrite & revParamOpt & pathEndOrSingleSlash & jsonEntity) {
                        case (None, payload)      =>
                          // Create a resolver with an id segment
                          emitMetadata(Created, resolvers.create(resolver, project, payload))
                        case (Some(rev), payload) =>
                          // Update a resolver
                          emitMetadata(resolvers.update(resolver, project, rev, payload))
                      },
                      (delete & authorizeWrite & revParam) { rev =>
                        // Deprecate a resolver
                        emitMetadataOrReject(resolvers.deprecate(resolver, project, rev))
                      },
                      // Fetches a resolver
                      (get & idSegmentRef(resolver)) { resolverRef =>
                        emitOrFusionRedirect(
                          project,
                          resolverRef,
                          authorizeRead {
                            emitFetch(resolvers.fetch(resolverRef, project))
                          }
                        )
                      }
                    )
                  },
                  // Fetches a resolver original source
                  (pathPrefix("source") & get & pathEndOrSingleSlash & idSegmentRef(resolver) & authorizeRead) {
                    resolverRef =>
                      emitSource(resolvers.fetch(resolverRef, project))
                  },
                  // Fetch a resource using a resolver
                  (get & idSegmentRef) { resourceIdRef =>
                    concat(
                      (pathEndOrSingleSlash & parameter("showReport".as[Boolean].withDefault(default = false))) {
                        showReport =>
                          val outputType =
                            if showReport then ResolvedResourceOutputType.Report else ResolvedResourceOutputType.JsonLd
                          resolveResource(resourceIdRef, project, resolutionType(resolver), outputType)
                      },
                      (pathPrefix("source") & pathEndOrSingleSlash & annotateSource) { annotate =>
                        resolveResource(
                          resourceIdRef,
                          project,
                          resolutionType(resolver),
                          if annotate then ResolvedResourceOutputType.AnnotatedSource
                          else ResolvedResourceOutputType.Source
                        )
                      }
                    )
                  }
                )
              }
            )
          }
        }
      }
    }

  private def resolveResource(
      resource: IdSegmentRef,
      project: ProjectRef,
      resolutionType: ResolutionType,
      output: ResolvedResourceOutputType
  )(using BaseUri, Caller): Route =
    authorizeFor(project, Permissions.resources.read).apply {
      exceptionHandler(enableRejects = false) {
        def emitResult[R: JsonLdEncoder: HttpResponseFields](io: IO[MultiResolutionResult[R]]) = {
          output match {
            case ResolvedResourceOutputType.Report          => emit(io.map(_.report))
            case ResolvedResourceOutputType.JsonLd          => emit(io.map(_.value.jsonLdValue))
            case ResolvedResourceOutputType.Source          => emit(io.map(_.value.source))
            case ResolvedResourceOutputType.AnnotatedSource =>
              emit(io.map { r => OriginalSource.annotated(r.value.resource, r.value.source) })
          }
        }

        resolutionType match {
          case ResolutionType.AllResolversInProject => emitResult(multiResolution(resource, project))
          case SingleResolver(resolver)             => emitResult(multiResolution(resource, project, resolver))
        }
      }
    }

  private def resolutionType(segment: IdSegment): ResolutionType = {
    underscoreToOption(segment) match {
      case Some(resolver) => SingleResolver(resolver)
      case None           => AllResolversInProject
    }
  }
}

sealed trait ResolutionType
object ResolutionType {
  case object AllResolversInProject        extends ResolutionType
  case class SingleResolver(id: IdSegment) extends ResolutionType
}

sealed trait ResolvedResourceOutputType
object ResolvedResourceOutputType {
  case object Report          extends ResolvedResourceOutputType
  case object JsonLd          extends ResolvedResourceOutputType
  case object Source          extends ResolvedResourceOutputType
  case object AnnotatedSource extends ResolvedResourceOutputType
}

object ResolversRoutes {

  /**
    * @return
    *   the [[Route]] for resolvers
    */
  def apply(
      identities: Identities,
      aclCheck: AclCheck,
      resolvers: Resolvers,
      multiResolution: MultiResolution,
      schemeDirectives: DeltaSchemeDirectives
  )(using BaseUri, RemoteContextResolution, JsonKeyOrdering, FusionConfig, Tracer[IO]): Route =
    new ResolversRoutes(identities, aclCheck, resolvers, multiResolution, schemeDirectives).routes

}
