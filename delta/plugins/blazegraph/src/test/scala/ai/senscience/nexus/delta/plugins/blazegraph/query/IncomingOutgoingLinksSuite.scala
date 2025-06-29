package ai.senscience.nexus.delta.plugins.blazegraph.query

import ai.senscience.nexus.delta.kernel.search.Pagination
import ai.senscience.nexus.delta.plugins.blazegraph.SparqlClientSetup
import ai.senscience.nexus.delta.plugins.blazegraph.client.SparqlClient
import ai.senscience.nexus.delta.plugins.blazegraph.model.SparqlLink
import ai.senscience.nexus.delta.plugins.blazegraph.model.SparqlLink.{SparqlExternalLink, SparqlResourceLink}
import ai.senscience.nexus.delta.plugins.blazegraph.query.IncomingOutgoingLinks.Queries
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.rdf.graph.NTriples
import ai.senscience.nexus.delta.rdf.syntax.iriStringContextSyntax
import ai.senscience.nexus.delta.sdk.model.search.ResultEntry.UnscoredResultEntry
import ai.senscience.nexus.delta.sdk.model.search.SearchResults.UnscoredSearchResults
import ai.senscience.nexus.delta.sdk.model.{BaseUri, ResourceAccess, ResourceF}
import ai.senscience.nexus.delta.sdk.projects.FetchContextDummy
import ai.senscience.nexus.delta.sdk.projects.model.{ApiMappings, ProjectContext}
import ai.senscience.nexus.delta.sourcing.model.{Identity, ProjectRef, ResourceRef}
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.effect.{IO, Resource}
import cats.syntax.all.*
import munit.AnyFixture
import munit.catseffect.IOFixture
import org.http4s.Uri

import java.time.Instant

class IncomingOutgoingLinksSuite extends NexusSuite with SparqlClientSetup.Fixture {

  private val queries: IOFixture[Queries] = ResourceSuiteLocalFixture("queries", Resource.eval(Queries.load))

  override def munitFixtures: Seq[AnyFixture[?]] = List(blazegraphClient, queries)

  implicit private val baseUri: BaseUri = BaseUri.unsafe("http://localhost", "v1")

  private val project          = ProjectRef.unsafe("org", "proj")
  private val incomingOutgoing = "incoming-outgoing"

  private val fetchContext = FetchContextDummy(
    Map(project -> ProjectContext.unsafe(ApiMappings.empty, nxv.base, nxv.base, enforceSchema = false))
  )

  private lazy val client: SparqlClient  = blazegraphClient()
  private lazy val incomingOutgoingLinks = IncomingOutgoingLinks(
    fetchContext,
    _ => IO.pure(incomingOutgoing),
    client,
    queries()
  )

  private val resource1Id = iri"https://bbp.epfl.ch/resource1"
  private val resource2Id = iri"https://bbp.epfl.ch/resource2"
  private val resource3Id = iri"https://bbp.epfl.ch/resource3"
  private val resource4Id = iri"https://bbp.epfl.ch/resource4"

  private def sparqlResourceLinkFor(resourceId: Iri, path: Iri): SparqlLink =
    SparqlResourceLink(
      ResourceF(
        resourceId,
        ResourceAccess.resource(project, resourceId),
        2,
        Set(resourceId / "type"),
        deprecated = false,
        Instant.EPOCH,
        Identity.Anonymous,
        Instant.EPOCH,
        Identity.Anonymous,
        ResourceRef(resourceId / "schema"),
        List(path)
      )
    )

  test("Create the name space and populate it") {
    def populateData =
      List(
        resource1Id -> "sparql/resource1.ntriples",
        resource2Id -> "sparql/resource2.ntriples",
        resource3Id -> "sparql/resource3.ntriples"
      ).traverse { case (rootNode, path) =>
        for {
          graphUri      <- IO.fromEither(Uri.fromString(rootNode.toString))
          ntriplesValue <- loader.contentOf(path)
          ntriples       = NTriples(ntriplesValue, rootNode)
          _             <- client.replace(incomingOutgoing, graphUri, ntriples)
        } yield ()
      }.void

    client.createNamespace(incomingOutgoing) >> populateData
  }

  test("Query incoming links") {
    val expected = UnscoredSearchResults(
      2,
      Seq(
        UnscoredResultEntry(sparqlResourceLinkFor(resource3Id, iri"https://bbp.epfl.ch/incoming")),
        UnscoredResultEntry(sparqlResourceLinkFor(resource2Id, iri"https://bbp.epfl.ch/incoming"))
      )
    )
    incomingOutgoingLinks
      .incoming(resource1Id, project, Pagination.OnePage)
      .assertEquals(expected)
  }

  test("Query outgoing links including external") {
    val expected: UnscoredSearchResults[SparqlLink] = UnscoredSearchResults(
      3,
      Seq(
        UnscoredResultEntry(sparqlResourceLinkFor(resource3Id, iri"https://bbp.epfl.ch/outgoing")),
        UnscoredResultEntry(sparqlResourceLinkFor(resource2Id, iri"https://bbp.epfl.ch/outgoing")),
        UnscoredResultEntry(SparqlExternalLink(resource4Id, List(iri"https://bbp.epfl.ch/outgoing")))
      )
    )
    incomingOutgoingLinks
      .outgoing(resource1Id, project, Pagination.OnePage, includeExternalLinks = true)
      .assertEquals(expected)
  }

  test("Query outgoing links excluding external") {
    val expected = UnscoredSearchResults(
      2,
      Seq(
        UnscoredResultEntry(sparqlResourceLinkFor(resource3Id, iri"https://bbp.epfl.ch/outgoing")),
        UnscoredResultEntry(sparqlResourceLinkFor(resource2Id, iri"https://bbp.epfl.ch/outgoing"))
      )
    )

    incomingOutgoingLinks
      .outgoing(resource1Id, project, Pagination.OnePage, includeExternalLinks = false)
      .assertEquals(expected)
  }

}
