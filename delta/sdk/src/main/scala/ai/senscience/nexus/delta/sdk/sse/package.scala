package ai.senscience.nexus.delta.sdk

import ai.senscience.nexus.delta.sourcing.model.Label
import cats.effect.IO
import fs2.Stream
import org.apache.pekko.http.scaladsl.model.sse.ServerSentEvent

package object sse {

  type ServerSentEventStream = Stream[IO, ServerSentEvent]

  val resourcesSelector = Label.unsafe("resources")

}
