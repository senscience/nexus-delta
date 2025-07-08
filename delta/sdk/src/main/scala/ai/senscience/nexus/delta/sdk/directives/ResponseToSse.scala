package ai.senscience.nexus.delta.sdk.directives

import ai.senscience.nexus.delta.sdk.sse.ServerSentEventStream
import ai.senscience.nexus.delta.sdk.stream.StreamConverter
import akka.http.scaladsl.marshalling.sse.EventStreamMarshalling.*
import akka.http.scaladsl.model.StatusCodes.OK
import akka.http.scaladsl.model.sse.ServerSentEvent
import akka.http.scaladsl.server.Directives.*
import akka.http.scaladsl.server.Route
import cats.effect.IO
import cats.effect.unsafe.implicits.*

import scala.concurrent.duration.*

sealed trait ResponseToSse {
  def apply(): Route
}

object ResponseToSse {

  private def apply(io: IO[ServerSentEventStream]): ResponseToSse =
    new ResponseToSse {
      override def apply(): Route =
        onSuccess(io.unsafeToFuture()) { stream =>
          complete(
            OK,
            StreamConverter.apply(stream).keepAlive(10.seconds, () => ServerSentEvent.heartbeat)
          )
        }
    }

  implicit def ioStream(io: IO[ServerSentEventStream]): ResponseToSse = ResponseToSse(io)

  implicit def streamValue(value: ServerSentEventStream): ResponseToSse = ResponseToSse(IO.pure(value))
}
