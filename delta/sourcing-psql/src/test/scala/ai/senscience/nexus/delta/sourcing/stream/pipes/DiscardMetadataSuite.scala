package ai.senscience.nexus.delta.sourcing.stream.pipes

import ai.senscience.nexus.delta.rdf.graph.Graph
import ai.senscience.nexus.testkit.mu.NexusSuite

class DiscardMetadataSuite extends NexusSuite with ElemFixtures {

  test("Discard metadata graph") {
    val expected = element.copy(value = graph.copy(metadataGraph = Graph.empty(base / "id")))

    DiscardMetadata
      .withConfig(())(element)
      .assertEquals(expected)
  }
}
