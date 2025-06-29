package ai.senscience.nexus.delta.sdk.directives

import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.emit
import ai.senscience.nexus.delta.sdk.directives.Response.{Complete, Reject}
import ai.senscience.nexus.delta.sdk.marshalling.HttpResponseFields
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

  private def apply[E: JsonLdEncoder, A](
      io: IO[Either[Response[E], ServerSentEventStream]]
  )(implicit jo: JsonKeyOrdering, cr: RemoteContextResolution): ResponseToSse =
    new ResponseToSse {

      override def apply(): Route =
        onSuccess(io.unsafeToFuture()) {
          case Left(complete: Complete[E]) => emit(complete)
          case Left(reject: Reject[E])     => emit(reject)
          case Right(stream)               =>
            complete(
              OK,
              StreamConverter.apply(stream).keepAlive(10.seconds, () => ServerSentEvent.heartbeat)
            )
        }
    }

  implicit def ioStream[E: JsonLdEncoder: HttpResponseFields](
      io: IO[Either[E, ServerSentEventStream]]
  )(implicit jo: JsonKeyOrdering, cr: RemoteContextResolution): ResponseToSse =
    ResponseToSse(io.map(_.left.map(Complete(_))))

  implicit def streamValue(
      value: ServerSentEventStream
  )(implicit jo: JsonKeyOrdering, cr: RemoteContextResolution): ResponseToSse =
    ResponseToSse(IO.pure(Right(value)))
}
