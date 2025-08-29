package ai.senscience.nexus.delta.sourcing.stream.pipes

import ai.senscience.nexus.delta.sourcing.PullRequest
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.stream.Elem.SuccessElem
import ai.senscience.nexus.testkit.mu.NexusSuite

class FilterDeprecatedSuite extends NexusSuite with ElemFixtures {

  def filterDeprecated: FilterDeprecated =
    FilterDeprecated.withConfig(())

  test("Drop deprecated elements") {
    val elem = SuccessElem(
      tpe = PullRequest.entityType,
      id = base / "id",
      project = project,
      instant = instant,
      offset = Offset.at(1L),
      value = graph.copy(deprecated = true),
      rev = 1
    )

    filterDeprecated(elem).assertEquals(elem.dropped)
  }

  test("Preserve non-deprecated elements") {
    val elem = SuccessElem(
      tpe = PullRequest.entityType,
      id = base / "id",
      project = project,
      instant = instant,
      offset = Offset.at(1L),
      value = graph,
      rev = 1
    )

    filterDeprecated(elem).assertEquals(elem)
  }
}
