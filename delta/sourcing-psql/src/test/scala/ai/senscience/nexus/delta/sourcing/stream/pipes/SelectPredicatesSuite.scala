package ai.senscience.nexus.delta.sourcing.stream.pipes

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.*
import ai.senscience.nexus.delta.rdf.graph.Graph
import ai.senscience.nexus.delta.rdf.implicits.*
import ai.senscience.nexus.delta.sourcing.stream.pipes.SelectPredicates.SelectPredicatesConfig
import ai.senscience.nexus.testkit.mu.NexusSuite

class SelectPredicatesSuite extends NexusSuite with ElemFixtures {

  private val pullRequestType: Iri = nxv + "PullRequest"
  private val fileType: Iri        = nxv + "File"
  private val fileGraph            = Graph
    .empty(base / "id")
    .add(rdf.tpe, fileType)
    .add(rdfs.label, "File name")
    .add(nxv + "fileSize", 42)
  private def fileElem             =
    element.copy(value = element.value.copy(graph = fileGraph, types = Set(fileType)))

  def selectPredicates(predicates: Set[Iri]): SelectPredicates =
    SelectPredicates.withConfig(SelectPredicatesConfig(None, predicates))

  val defaultPreficates: SelectPredicates = DefaultLabelPredicates.withConfig(())

  test("Produce an empty graph if the predicate set is empty") {
    val expected = element.copy(value = element.value.copy(graph = Graph.empty(base / "id"), types = Set.empty))
    selectPredicates(Set.empty)(element).assertEquals(expected)
  }

  test(s"Retain only the '${rdfs.label}' predicate") {
    val graph    = Graph.empty(base / "id").add(rdfs.label, "active")
    val expected = element.copy(value = element.value.copy(graph = graph, types = Set.empty))
    selectPredicates(Set(rdfs.label))(element).assertEquals(expected)
  }

  test(s"Retain the '${rdfs.label}' and the '${rdf.tpe}' predicate with the default pipe") {
    val graph    = Graph.empty(base / "id").add(rdfs.label, "active").add(rdf.tpe, pullRequestType)
    val expected = element.copy(value = element.value.copy(graph = graph, types = Set(pullRequestType)))
    defaultPreficates(element).assertEquals(expected)
  }

  test(s"Retain only the '${rdfs.label}' predicate for a file if '$fileType' is not a forward type") {
    val graph    = Graph.empty(base / "id").add(rdfs.label, "File name")
    val expected = fileElem.copy(value = fileElem.value.copy(graph = graph, types = Set.empty))
    selectPredicates(Set(rdfs.label))(fileElem).assertEquals(expected)
  }

  test(s"Do not apply any modifications for a forward type '$fileType' for the default predicate") {
    defaultPreficates(fileElem).assertEquals(fileElem)
  }
}
