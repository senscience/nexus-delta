package ai.senscience.nexus.delta.plugins.blazegraph.routes

import ai.senscience.nexus.delta.plugins.blazegraph.Fixtures
import ai.senscience.nexus.delta.plugins.blazegraph.model.SparqlLink.{SparqlExternalLink, SparqlResourceLink}
import ai.senscience.nexus.delta.plugins.blazegraph.model.{schema, SparqlLink}
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.ConfigFixtures
import ai.senscience.nexus.delta.sdk.acls.AclSimpleCheck
import ai.senscience.nexus.delta.sdk.generators.ProjectGen
import ai.senscience.nexus.delta.sdk.identities.IdentitiesDummy
import ai.senscience.nexus.delta.sdk.marshalling.{RdfExceptionHandler, RdfRejectionHandler}
import ai.senscience.nexus.delta.sdk.model.search.ResultEntry.UnscoredResultEntry
import ai.senscience.nexus.delta.sdk.model.search.SearchResults.UnscoredSearchResults
import ai.senscience.nexus.delta.sdk.model.search.{PaginationConfig, SearchResults}
import ai.senscience.nexus.delta.sdk.model.{BaseUri, ResourceAccess, ResourceF}
import ai.senscience.nexus.delta.sdk.projects.model.ApiMappings
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sdk.utils.RouteHelpers
import ai.senscience.nexus.delta.sourcing.model.Identity.User
import ai.senscience.nexus.delta.sourcing.model.{Identity, Label, ResourceRef}
import ai.senscience.nexus.testkit.scalatest.ce.CatsEffectSpec
import ai.senscience.nexus.testkit.{CirceEq, CirceLiteral}
import akka.http.scaladsl.server.{ExceptionHandler, RejectionHandler}
import org.scalatest.{BeforeAndAfterAll, CancelAfterFailure, Inspectors}

import java.time.Instant
import java.util.UUID

trait BlazegraphViewRoutesFixtures
    extends CatsEffectSpec
    with RouteHelpers
    with CirceLiteral
    with CirceEq
    with Inspectors
    with CancelAfterFailure
    with ConfigFixtures
    with BeforeAndAfterAll
    with Fixtures {

  implicit val baseUri: BaseUri = BaseUri.unsafe("http://localhost", "v1")

  implicit val ordering: JsonKeyOrdering          =
    JsonKeyOrdering.default(topKeys =
      List("@context", "@id", "@type", "reason", "details", "sourceId", "projectionId", "_total", "_results")
    )
  implicit val rejectionHandler: RejectionHandler = RdfRejectionHandler.apply
  implicit val exceptionHandler: ExceptionHandler = RdfExceptionHandler.apply

  implicit val paginationConfig: PaginationConfig = pagination

  val uuid = UUID.randomUUID()

  val aclCheck = AclSimpleCheck().accepted

  val realm = Label.unsafe("myrealm")

  val reader = User("reader", realm)
  val writer = User("writer", realm)
  val admin  = User("admin", realm)

  val identities = IdentitiesDummy.fromUsers(reader, writer, admin)

  val org           = Label.unsafe("org")
  val orgDeprecated = Label.unsafe("org-deprecated")
  val base          = nxv.base
  val mappings      = ApiMappings("example" -> iri"http://example.com/", "view" -> schema.iri)

  val project                  = ProjectGen.project("org", "proj", base = base, mappings = mappings)
  val deprecatedProject        = ProjectGen.project("org", "proj-deprecated")
  val projectWithDeprecatedOrg = ProjectGen.project("org-deprecated", "other-proj")
  val projectRef               = project.ref

  val linksResults: SearchResults[SparqlLink] = UnscoredSearchResults(
    2,
    List(
      UnscoredResultEntry(
        SparqlResourceLink(
          ResourceF(
            iri"http://example.com/id1",
            ResourceAccess.resource(projectRef, iri"http://example.com/id1"),
            1,
            Set(iri"http://example.com/type1", iri"http://example.com/type2"),
            false,
            Instant.EPOCH,
            Identity.Anonymous,
            Instant.EPOCH,
            Identity.Anonymous,
            ResourceRef(iri"http://example.com/someSchema"),
            List(iri"http://example.com/property1", iri"http://example.com/property2")
          )
        )
      ),
      UnscoredResultEntry(
        SparqlExternalLink(
          iri"http://example.com/external",
          List(iri"http://example.com/property3", iri"http://example.com/property4"),
          Set(iri"http://example.com/type3", iri"http://example.com/type4")
        )
      )
    )
  )
}
