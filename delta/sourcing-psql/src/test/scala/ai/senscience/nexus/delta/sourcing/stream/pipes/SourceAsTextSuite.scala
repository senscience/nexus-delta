package ai.senscience.nexus.delta.sourcing.stream.pipes

import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.rdf.implicits.given
import ai.senscience.nexus.delta.sourcing.PullRequest
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.stream.Elem.SuccessElem
import ai.senscience.nexus.testkit.mu.NexusSuite
import io.circe.Json

class SourceAsTextSuite extends NexusSuite with ElemFixtures {

  test("Embed the source to the metadata graph") {
    val elem             = SuccessElem(
      tpe = PullRequest.entityType,
      id = base / "id",
      project = project,
      instant = instant,
      offset = Offset.at(1L),
      value = graph,
      rev = 1
    )
    val newMetadataGraph = graph.metadataGraph.add(nxv.originalSource.iri, graph.source.noSpaces)
    val expected         = elem.copy(value = graph.copy(metadataGraph = newMetadataGraph, source = Json.obj()))
    SourceAsText.withConfig(())(elem).assertEquals(expected)
  }
}
