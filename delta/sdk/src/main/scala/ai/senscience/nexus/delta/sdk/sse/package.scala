package ai.senscience.nexus.delta.sdk

import ai.senscience.nexus.delta.sourcing.model.Label
import akka.http.scaladsl.model.sse.ServerSentEvent
import cats.effect.IO
import fs2.Stream

package object sse {

  type ServerSentEventStream = Stream[IO, ServerSentEvent]

  val resourcesSelector = Label.unsafe("resources")

}
