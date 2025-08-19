package ai.senscience.nexus.delta.sdk.directives

import ai.senscience.nexus.delta.sdk.directives.Response.Complete
import ai.senscience.nexus.delta.sdk.marshalling.RdfMarshalling
import akka.http.scaladsl.marshalling.ToEntityMarshaller
import akka.http.scaladsl.model.StatusCode
import akka.http.scaladsl.model.StatusCodes.OK
import akka.http.scaladsl.server.Directives.{complete, onSuccess}
import akka.http.scaladsl.server.Route
import cats.effect.IO
import cats.effect.unsafe.implicits.*

trait ResponseToMarshaller {
  def apply(statusOverride: Option[StatusCode]): Route
}

object ResponseToMarshaller extends RdfMarshalling {

  private[directives] def apply[A: ToEntityMarshaller](io: IO[Complete[A]]): ResponseToMarshaller =
    (statusOverride: Option[StatusCode]) => {

      val ioFinal = io.map(value => value.copy(status = statusOverride.getOrElse(value.status)))
      val ioRoute = ioFinal.map { v =>
        complete(v.status, v.headers, v.value)
      }
      onSuccess(ioRoute.unsafeToFuture())(identity)
    }

  implicit def ioEntityMarshaller[A: ToEntityMarshaller](io: IO[A]): ResponseToMarshaller =
    ResponseToMarshaller(io.map(v => Complete(OK, Seq.empty, None, v)))
}
