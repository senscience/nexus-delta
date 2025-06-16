package ai.senscience.nexus.delta.plugins.blazegraph.indexing

import ai.senscience.nexus.delta.plugins.blazegraph.client.SparqlClient
import munit.AnyFixture

class BlazegraphSinkSuite extends SparqlSinkSuite {

  override def munitFixtures: Seq[AnyFixture[?]] = List(blazegraphClient)

  override lazy val client: SparqlClient = blazegraphClient()

}
