package ai.senscience.nexus.delta.sourcing.stream.pipes

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.syntax.iriStringContextSyntax
import ai.senscience.nexus.delta.sourcing.PullRequest
import ai.senscience.nexus.delta.sourcing.PullRequest.PullRequestState
import ai.senscience.nexus.delta.sourcing.PullRequest.PullRequestState.PullRequestActive
import ai.senscience.nexus.delta.sourcing.model.Identity.Anonymous
import ai.senscience.nexus.delta.sourcing.model.{IriFilter, Label, ProjectRef}
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.state.GraphResource
import ai.senscience.nexus.delta.sourcing.stream.Elem.SuccessElem
import ai.senscience.nexus.delta.sourcing.stream.ReferenceRegistry
import ai.senscience.nexus.delta.sourcing.stream.pipes.FilterByType.FilterByTypeConfig
import ch.epfl.bluebrain.nexus.testkit.mu.NexusSuite

import java.time.Instant

class FilterByTypeSuite extends NexusSuite {

  private val base    = iri"http://localhost"
  private val instant = Instant.now()
  private val project = ProjectRef(Label.unsafe("org"), Label.unsafe("proj"))
  private val state   = PullRequestActive(
    id = base / "id",
    project = project,
    rev = 1,
    createdAt = instant,
    createdBy = Anonymous,
    updatedAt = instant,
    updatedBy = Anonymous
  )
  private val graph   = PullRequestState.toGraphResource(state, base)

  private val registry = new ReferenceRegistry
  registry.register(FilterByType)

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

  def pipe(types: Set[Iri]): FilterByType =
    registry
      .lookupA[FilterByType.type](FilterByType.ref)
      .rightValue
      .withJsonLdConfig(FilterByTypeConfig(IriFilter.fromSet(types)).toJsonLd)
      .rightValue

  test("Do not filter elements if the expected type set is empty") {
    val elem = element(Set(iri"http://localhost/tpe1"))
    pipe(Set.empty).apply(elem).assertEquals(elem)
  }

  test("Do not filter elements if the expected and elem type set is empty") {
    val elem = element(Set.empty)
    pipe(Set.empty).apply(elem).assertEquals(elem)
  }

  test("Do not filter elements if the type intersection is not void") {
    val elem = element(Set(iri"http://localhost/tpe1", iri"http://localhost/tpe2"))
    pipe(Set(iri"http://localhost/tpe2", iri"http://localhost/tpe3")).apply(elem).assertEquals(elem)
  }

  test("Filter elements if the type intersection is void") {
    val elem = element(Set(iri"http://localhost/tpe1"))
    pipe(Set(iri"http://localhost/tpe2", iri"http://localhost/tpe3")).apply(elem).assertEquals(elem.dropped)
  }
}
