package ai.senscience.nexus.delta.plugins.blazegraph.indexing

import ai.senscience.nexus.delta.plugins.blazegraph.client.SparqlClient
import munit.AnyFixture

class RD4JSinkSuite extends SparqlSinkSuite {

  override def munitFixtures: Seq[AnyFixture[?]] = List(rdf4jClient)

  override lazy val client: SparqlClient = rdf4jClient()

}
