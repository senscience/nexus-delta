package ai.senscience.nexus.delta.sdk.utils

import ai.senscience.nexus.delta.rdf.jsonld.api.{JsonLdApi, TitaniumJsonLdApi}
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.fusion.FusionConfig
import ai.senscience.nexus.delta.sdk.marshalling.{RdfExceptionHandler, RdfRejectionHandler}
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.model.search.PaginationConfig
import ai.senscience.nexus.delta.sdk.*
import ai.senscience.nexus.delta.sourcing.model.Identity.User
import ai.senscience.nexus.delta.sourcing.model.Label
import cats.effect.IO
import org.apache.pekko.http.scaladsl.server.{ExceptionHandler, RejectionHandler}
import org.http4s.syntax.literals.uri
import org.typelevel.otel4s.trace.Tracer

trait RouteFixtures extends RemoteContextResolutionFixtures {

  given api: JsonLdApi     = TitaniumJsonLdApi.strict
  private given Tracer[IO] = Tracer.noop[IO]

  def extraContexts: RemoteContextResolution = RemoteContextResolution.never

  given rcr: RemoteContextResolution =
    loadCoreContexts(
      acls.contexts.definition ++
        identities.contexts.definition ++
        organizations.contexts.definition ++
        permissions.contexts.definition ++
        projects.contexts.definition ++
        realms.contexts.definition ++
        resolvers.contexts.definition ++
        schemas.contexts.definition ++
        typehierarchy.contexts.definition
    ).merge(extraContexts)

  given ordering: JsonKeyOrdering =
    JsonKeyOrdering.default(topKeys =
      List("@context", "@id", "@type", "reason", "details", "sourceId", "projectionId", "_total", "_results")
    )

  given baseUri: BaseUri                   = BaseUri.unsafe("http://localhost", "v1")
  given paginationConfig: PaginationConfig = PaginationConfig(5, 10, 5)
  given f: FusionConfig                    =
    FusionConfig(uri"https://bbp.epfl.ch/nexus/web/", enableRedirects = true, uri"https://bbp.epfl.ch")
  given rejectionHandler: RejectionHandler = RdfRejectionHandler.apply
  given exceptionHandler: ExceptionHandler = RdfExceptionHandler.apply

  val realm: Label = Label.unsafe("wonderland")
  val alice: User  = User("alice", realm)
  val bob: User    = User("bob", realm)
}
