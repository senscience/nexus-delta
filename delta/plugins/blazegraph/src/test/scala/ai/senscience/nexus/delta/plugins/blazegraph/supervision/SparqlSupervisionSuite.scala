package ai.senscience.nexus.delta.plugins.blazegraph.supervision

import ai.senscience.nexus.delta.plugins.blazegraph.SparqlClientSetup
import ai.senscience.nexus.delta.plugins.blazegraph.supervision.SparqlSupervision.SparqlNamespaceTriples
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sdk.views.ViewRef
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.effect.IO
import munit.AnyFixture

class SparqlSupervisionSuite extends NexusSuite with SparqlClientSetup.Fixture {

  override def munitFixtures: Seq[AnyFixture[?]] = List(blazegraphClient)

  private val project = ProjectRef.unsafe("org", "project")
  private val first   = ViewRef(project, nxv + "first")
  private val second  = ViewRef(project, nxv + "second")

  private lazy val client                       = blazegraphClient()
  private val viewsByNamespace: ViewByNamespace = new ViewByNamespace {
    override def get: IO[Map[String, ViewRef]] = IO.pure(Map("first" -> first, "second" -> second))
  }

  private lazy val supervision = SparqlSupervision(client, viewsByNamespace)

  test("Return the supervision for the different namespaces") {
    val expected = SparqlNamespaceTriples(
      0L,
      Map(first -> 0L, second    -> 0L),
      Map("kb"  -> 0L, "unknown" -> 0L)
    )

    client.createNamespace("first") >>
      client.createNamespace("second") >>
      client.createNamespace("unknown") >> supervision.get.assertEquals(expected)

  }

}
