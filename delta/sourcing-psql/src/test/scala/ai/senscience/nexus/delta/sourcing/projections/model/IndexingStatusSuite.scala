package ai.senscience.nexus.delta.sourcing.projections.model

import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.testkit.mu.NexusSuite

class IndexingStatusSuite extends NexusSuite {

  private val projectionOffset = Offset.at(5L)

  test("Smaller resource offset returns a completed status") {
    assertEquals(
      IndexingStatus.fromOffsets(projectionOffset, Offset.at(4L)),
      IndexingStatus.Completed
    )
  }

  test("Same resource offset returns a completed status") {
    assertEquals(
      IndexingStatus.fromOffsets(projectionOffset, projectionOffset),
      IndexingStatus.Completed
    )
  }

  test("Larger resource offset returns a pending status") {
    assertEquals(
      IndexingStatus.fromOffsets(projectionOffset, Offset.at(6L)),
      IndexingStatus.Pending
    )
  }

}
