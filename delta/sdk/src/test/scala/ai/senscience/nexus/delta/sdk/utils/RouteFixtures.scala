package ai.senscience.nexus.delta.sdk.utils

import ai.senscience.nexus.delta.rdf.Vocabulary.contexts
import ai.senscience.nexus.delta.rdf.jsonld.api.{JsonLdApi, TitaniumJsonLdApi}
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.fusion.FusionConfig
import ai.senscience.nexus.delta.sdk.marshalling.{RdfExceptionHandler, RdfRejectionHandler}
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.model.search.PaginationConfig
import ai.senscience.nexus.delta.sourcing.model.Identity.User
import ai.senscience.nexus.delta.sourcing.model.Label
import ai.senscience.nexus.testkit.scalatest.ClasspathResources
import akka.http.scaladsl.server.{ExceptionHandler, RejectionHandler}
import org.http4s.implicits.http4sLiteralsSyntax

trait RouteFixtures {
  self: ClasspathResources =>

  implicit val api: JsonLdApi = TitaniumJsonLdApi.strict

  implicit def rcr: RemoteContextResolution =
    RemoteContextResolution.fixedIO(
      contexts.acls                  -> ContextValue.fromFile("contexts/acls.json"),
      contexts.aclsMetadata          -> ContextValue.fromFile("contexts/acls-metadata.json"),
      contexts.metadata              -> ContextValue.fromFile("contexts/metadata.json"),
      contexts.error                 -> ContextValue.fromFile("contexts/error.json"),
      contexts.validation            -> ContextValue.fromFile("contexts/validation.json"),
      contexts.organizations         -> ContextValue.fromFile("contexts/organizations.json"),
      contexts.organizationsMetadata -> ContextValue.fromFile("contexts/organizations-metadata.json"),
      contexts.identities            -> ContextValue.fromFile("contexts/identities.json"),
      contexts.permissions           -> ContextValue.fromFile("contexts/permissions.json"),
      contexts.permissionsMetadata   -> ContextValue.fromFile("contexts/permissions-metadata.json"),
      contexts.projects              -> ContextValue.fromFile("contexts/projects.json"),
      contexts.projectsMetadata      -> ContextValue.fromFile("contexts/projects-metadata.json"),
      contexts.realms                -> ContextValue.fromFile("contexts/realms.json"),
      contexts.realmsMetadata        -> ContextValue.fromFile("contexts/realms-metadata.json"),
      contexts.remoteContexts        -> ContextValue.fromFile("contexts/remote-contexts.json"),
      contexts.resolvers             -> ContextValue.fromFile("contexts/resolvers.json"),
      contexts.resolversMetadata     -> ContextValue.fromFile("contexts/resolvers-metadata.json"),
      contexts.search                -> ContextValue.fromFile("contexts/search.json"),
      contexts.shacl                 -> ContextValue.fromFile("contexts/shacl.json"),
      contexts.schemasMetadata       -> ContextValue.fromFile("contexts/schemas-metadata.json"),
      contexts.offset                -> ContextValue.fromFile("contexts/offset.json"),
      contexts.statistics            -> ContextValue.fromFile("contexts/statistics.json"),
      contexts.supervision           -> ContextValue.fromFile("contexts/supervision.json"),
      contexts.suites                -> ContextValue.fromFile("contexts/suites.json"),
      contexts.tags                  -> ContextValue.fromFile("contexts/tags.json"),
      contexts.typeHierarchy         -> ContextValue.fromFile("contexts/type-hierarchy.json"),
      contexts.version               -> ContextValue.fromFile("contexts/version.json")
    )

  implicit val ordering: JsonKeyOrdering =
    JsonKeyOrdering.default(topKeys =
      List("@context", "@id", "@type", "reason", "details", "sourceId", "projectionId", "_total", "_results")
    )

  implicit val baseUri: BaseUri                   = BaseUri.unsafe("http://localhost", "v1")
  implicit val paginationConfig: PaginationConfig = PaginationConfig(5, 10, 5)
  implicit val f: FusionConfig                    =
    FusionConfig(uri"https://bbp.epfl.ch/nexus/web/", enableRedirects = true, uri"https://bbp.epfl.ch")
  implicit val rejectionHandler: RejectionHandler = RdfRejectionHandler.apply
  implicit val exceptionHandler: ExceptionHandler = RdfExceptionHandler.apply

  val realm: Label = Label.unsafe("wonderland")
  val alice: User  = User("alice", realm)
  val bob: User    = User("bob", realm)
}
