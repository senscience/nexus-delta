package ai.senscience.nexus.delta.sdk.sse

import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.ConfigFixtures
import ai.senscience.nexus.delta.sdk.sse.SseEncoder.SseData
import ai.senscience.nexus.delta.sourcing.model.{EntityType, ProjectRef}
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.stream.Elem
import ai.senscience.nexus.testkit.mu.NexusSuite
import io.circe.JsonObject
import io.circe.syntax.KeyOps
import org.apache.pekko.http.scaladsl.model.sse.ServerSentEvent

import java.time.Instant

class SseEventLogSuite extends NexusSuite with ConfigFixtures {

  implicit private val jo: JsonKeyOrdering = JsonKeyOrdering.alphabetical

  private val project = ProjectRef.unsafe("org", "proj")

  private def makeElem(sseData: SseData) = Elem.SuccessElem(
    EntityType("Person"),
    nxv + "1",
    project,
    Instant.now(),
    Offset.at(5L),
    sseData,
    4
  )

  test("Should serialize to an Pekko SSE") {
    val elem = makeElem(
      SseData("Person", None, JsonObject("name" := "John Doe"))
    )
    assertEquals(
      SseEventLog.toServerSentEvent(elem),
      ServerSentEvent("""{"name":"John Doe"}""", "Person", "5")
    )
  }
}
