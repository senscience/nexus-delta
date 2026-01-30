package ai.senscience.nexus.delta.sourcing.stream.pipes

import ai.senscience.nexus.delta.rdf.Vocabulary.rdfs
import ai.senscience.nexus.delta.rdf.graph.Graph
import ai.senscience.nexus.delta.rdf.implicits.given
import ai.senscience.nexus.delta.rdf.query.SparqlQuery.SparqlConstructQuery
import ai.senscience.nexus.delta.sourcing.stream.pipes.DataConstructQuery.DataConstructQueryConfig
import ai.senscience.nexus.testkit.mu.NexusSuite

class DataConstructQuerySuite extends NexusSuite with ElemFixtures {

  test("Produce a correct graph") {
    val query         =
      """prefix nxv: <https://bluebrain.github.io/nexus/vocabulary/>
         |prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
         |
         |CONSTRUCT {
         |  ?resource rdfs:label ?upper_label .
         |}
         |WHERE {
         |  ?resource          a nxv:PullRequest ;
         |            rdfs:label          ?label .
         |  BIND(UCASE(?label) as ?upper_label) .
         |}""".stripMargin
    val expectedGraph = Graph.empty(base / "id").add(rdfs.label, "ACTIVE")
    val expected      = element.copy(value = element.value.copy(graph = expectedGraph))
    val config        = DataConstructQueryConfig(SparqlConstructQuery.unsafe(query))
    DataConstructQuery
      .withConfig(config)(element)
      .assertEquals(expected)
  }

}
