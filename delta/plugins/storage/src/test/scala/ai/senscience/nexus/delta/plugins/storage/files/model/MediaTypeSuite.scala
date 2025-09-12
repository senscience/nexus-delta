package ai.senscience.nexus.delta.plugins.storage.files.model

import ai.senscience.nexus.testkit.mu.NexusSuite
import io.circe.Json

class MediaTypeSuite extends NexusSuite {

  test("Should deserialize legacy pekko content types") {
    Json
      .fromString("text/plain; charset=UTF-8")
      .as[MediaType]
      .assertRight(
        MediaType.`text/plain`
      )
  }

}
