package ai.senscience.nexus.delta.sourcing.stream.pipes

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.syntax.*
import ai.senscience.nexus.delta.sourcing.PullRequest
import ai.senscience.nexus.delta.sourcing.model.ResourceRef.{Latest, Revision}
import ai.senscience.nexus.delta.sourcing.model.{IriFilter, ResourceRef}
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.state.GraphResource
import ai.senscience.nexus.delta.sourcing.stream.Elem.SuccessElem
import ai.senscience.nexus.delta.sourcing.stream.pipes.FilterBySchema.FilterBySchemaConfig
import ai.senscience.nexus.testkit.mu.NexusSuite

class FilterBySchemaSuite extends NexusSuite with ElemFixtures {

  def element(schema: ResourceRef): SuccessElem[GraphResource] =
    SuccessElem(
      tpe = PullRequest.entityType,
      id = base / "id",
      project = project,
      instant = instant,
      offset = Offset.at(1L),
      value = graph.copy(schema = schema),
      rev = 1
    )

  def filterSchema(schemas: Set[Iri]): FilterBySchema =
    FilterBySchema.withConfig(FilterBySchemaConfig(IriFilter.fromSet(schemas)))

  test("Do not filter elements if the expected schema set is empty") {
    val elem = element(Latest(iri"http://localhost/schema1"))
    filterSchema(Set.empty).apply(elem).assertEquals(elem)
  }

  test("Do not filter elements if the schema intersection is not void") {
    val elem = element(Revision(iri"http://localhost/schema1", 2))
    filterSchema(Set(iri"http://localhost/schema1", iri"http://localhost/schema2")).apply(elem).assertEquals(elem)
  }

  test("Filter elements if the type intersection is void") {
    val elem = element(Revision(iri"http://localhost/schema1", 2))
    filterSchema(Set(iri"http://localhost/schema2", iri"http://localhost/schema3"))
      .apply(elem)
      .assertEquals(elem.dropped)
  }
}
