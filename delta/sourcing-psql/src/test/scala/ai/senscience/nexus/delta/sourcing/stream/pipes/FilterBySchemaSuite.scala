package ai.senscience.nexus.delta.sourcing.stream.pipes

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.syntax.iriStringContextSyntax
import ai.senscience.nexus.delta.sourcing.PullRequest
import ai.senscience.nexus.delta.sourcing.PullRequest.PullRequestState
import ai.senscience.nexus.delta.sourcing.PullRequest.PullRequestState.PullRequestActive
import ai.senscience.nexus.delta.sourcing.model.Identity.Anonymous
import ai.senscience.nexus.delta.sourcing.model.ResourceRef.{Latest, Revision}
import ai.senscience.nexus.delta.sourcing.model.{IriFilter, Label, ProjectRef, ResourceRef}
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.state.GraphResource
import ai.senscience.nexus.delta.sourcing.stream.Elem.SuccessElem
import ai.senscience.nexus.delta.sourcing.stream.ReferenceRegistry
import ai.senscience.nexus.delta.sourcing.stream.pipes.FilterBySchema.FilterBySchemaConfig
import ai.senscience.nexus.testkit.mu.NexusSuite

import java.time.Instant

class FilterBySchemaSuite extends NexusSuite {

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
  registry.register(FilterBySchema)

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

  def pipe(schemas: Set[Iri]): FilterBySchema =
    registry
      .lookupA[FilterBySchema.type](FilterBySchema.ref)
      .rightValue
      .withJsonLdConfig(FilterBySchemaConfig(IriFilter.fromSet(schemas)).toJsonLd)
      .rightValue

  test("Do not filter elements if the expected schema set is empty") {
    val elem = element(Latest(iri"http://localhost/schema1"))
    pipe(Set.empty).apply(elem).assertEquals(elem)
  }

  test("Do not filter elements if the schema intersection is not void") {
    val elem = element(Revision(iri"http://localhost/schema1", 2))
    pipe(Set(iri"http://localhost/schema1", iri"http://localhost/schema2")).apply(elem).assertEquals(elem)
  }

  test("Filter elements if the type intersection is void") {
    val elem = element(Revision(iri"http://localhost/schema1", 2))
    pipe(Set(iri"http://localhost/schema2", iri"http://localhost/schema3")).apply(elem).assertEquals(elem.dropped)
  }
}
