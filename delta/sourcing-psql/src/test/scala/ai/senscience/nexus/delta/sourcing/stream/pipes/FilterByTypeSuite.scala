package ai.senscience.nexus.delta.sourcing.stream.pipes

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.syntax.iriStringContextSyntax
import ai.senscience.nexus.delta.sourcing.PullRequest
import ai.senscience.nexus.delta.sourcing.model.IriFilter
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.state.GraphResource
import ai.senscience.nexus.delta.sourcing.stream.Elem.SuccessElem
import ai.senscience.nexus.delta.sourcing.stream.pipes.FilterByType.FilterByTypeConfig
import ai.senscience.nexus.testkit.mu.NexusSuite

class FilterByTypeSuite extends NexusSuite with ElemFixtures {

  def element(types: Set[Iri]): SuccessElem[GraphResource] =
    SuccessElem(
      tpe = PullRequest.entityType,
      id = base / "id",
      project = project,
      instant = instant,
      offset = Offset.at(1L),
      value = graph.copy(types = types),
      rev = 1
    )

  def filterByType(types: Set[Iri]): FilterByType =
    FilterByType.withConfig(FilterByTypeConfig(IriFilter.fromSet(types)))

  test("Do not filter elements if the expected type set is empty") {
    val elem = element(Set(iri"http://localhost/tpe1"))
    filterByType(Set.empty).apply(elem).assertEquals(elem)
  }

  test("Do not filter elements if the expected and elem type set is empty") {
    val elem = element(Set.empty)
    filterByType(Set.empty).apply(elem).assertEquals(elem)
  }

  test("Do not filter elements if the type intersection is not void") {
    val elem = element(Set(iri"http://localhost/tpe1", iri"http://localhost/tpe2"))
    filterByType(Set(iri"http://localhost/tpe2", iri"http://localhost/tpe3")).apply(elem).assertEquals(elem)
  }

  test("Filter elements if the type intersection is void") {
    val elem = element(Set(iri"http://localhost/tpe1"))
    filterByType(Set(iri"http://localhost/tpe2", iri"http://localhost/tpe3")).apply(elem).assertEquals(elem.dropped)
  }
}
