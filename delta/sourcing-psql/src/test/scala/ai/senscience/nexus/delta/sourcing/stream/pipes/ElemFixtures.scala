package ai.senscience.nexus.delta.sourcing.stream.pipes

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.syntax.*
import ai.senscience.nexus.delta.sourcing.PullRequest
import ai.senscience.nexus.delta.sourcing.PullRequest.PullRequestState
import ai.senscience.nexus.delta.sourcing.PullRequest.PullRequestState.PullRequestActive
import ai.senscience.nexus.delta.sourcing.model.Identity.Anonymous
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.state.GraphResource
import ai.senscience.nexus.delta.sourcing.stream.Elem.SuccessElem

import java.time.Instant

protected[pipes] trait ElemFixtures {

  val base: Iri            = iri"http://localhost"
  val instant: Instant     = Instant.now()
  val project: ProjectRef  = ProjectRef.unsafe("org", "proj")
  private val state        = PullRequestActive(
    id = base / "id",
    project = project,
    rev = 1,
    createdAt = instant,
    createdBy = Anonymous,
    updatedAt = instant,
    updatedBy = Anonymous
  )
  def graph: GraphResource = PullRequestState.toGraphResource(state, base)

  def element: SuccessElem[GraphResource] =
    SuccessElem(
      tpe = PullRequest.entityType,
      id = base / "id",
      project = project,
      instant = instant,
      offset = Offset.at(1L),
      value = graph,
      rev = 1
    )

}
