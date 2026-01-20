package ai.senscience.nexus.delta.rdf.query

import ai.senscience.nexus.delta.rdf.query.SparqlQuery.SparqlConstructQuery
import ai.senscience.nexus.testkit.mu.NexusSuite

class SparqlQuerySuite extends NexusSuite {

  val validSparql = """SELECT ?s ?p ?o WHERE { ?s ?p ?o }"""

  val validConstruct =
    """
                prefix example: <http://example.com/>
                prefix schema: <https://schema.org/>
  
                CONSTRUCT {
                  ?person 	        a                       ?type ;
                                   schema:name             ?name ;
                                   schema:birthDate        ?birthDate ;
                } WHERE {
                  ?person 	        a                       ?type ;
                                  schema:name             ?name ;
                                   schema:birthDate        ?birthDate ;
                }
                """

  test("SparqlQuery gives a construct query when a valid one is passed to apply") {
    assertEquals(SparqlQuery(validConstruct), SparqlConstructQuery.unsafe(validConstruct))
  }

  test("SparqlConstructQuery gives a construct query when a valid one is passed") {
    SparqlConstructQuery(validConstruct).assertRight(SparqlConstructQuery.unsafe(validConstruct))
  }

  test("SparqlConstructQuery raises an error when a non-construct query is passed") {
    SparqlConstructQuery(validSparql).assertLeft("The provided query is a valid SPARQL query but not a CONSTRUCT query")

  }

  test("SparqlConstructQuery raises an error when an invalid query is passed") {
    SparqlConstructQuery("xxx").assertLeft("The provided query is not a valid SPARQL query")
  }
}
