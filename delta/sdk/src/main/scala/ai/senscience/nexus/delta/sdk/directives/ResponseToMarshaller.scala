package ai.senscience.nexus.delta.sdk.directives

import ai.senscience.nexus.delta.rdf.jsonld.api.{JsonLdApi, TitaniumJsonLdApi}
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.directives.Response.{Complete, Reject}
import ai.senscience.nexus.delta.sdk.marshalling.{HttpResponseFields, RdfMarshalling}
import ai.senscience.nexus.delta.sdk.syntax.*
import akka.http.scaladsl.marshalling.ToEntityMarshaller
import akka.http.scaladsl.model.StatusCode
import akka.http.scaladsl.model.StatusCodes.OK
import akka.http.scaladsl.server.Directives.{complete, onSuccess, reject}
import akka.http.scaladsl.server.Route
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*

trait ResponseToMarshaller {
  def apply(statusOverride: Option[StatusCode]): Route
}

object ResponseToMarshaller extends RdfMarshalling {

  // To serialize errors to compacted json-ld
  implicit val api: JsonLdApi = TitaniumJsonLdApi.lenient

  private[directives] def apply[E: JsonLdEncoder, A: ToEntityMarshaller](
      io: IO[Either[Response[E], Complete[A]]]
  )(implicit cr: RemoteContextResolution, jo: JsonKeyOrdering): ResponseToMarshaller =
    (statusOverride: Option[StatusCode]) => {

      val ioFinal = io.map(_.map(value => value.copy(status = statusOverride.getOrElse(value.status))))

      val ioRoute = ioFinal.flatMap {
        case Left(r: Reject[E])    => IO.pure(reject(r))
        case Left(e: Complete[E])  => e.value.toCompactedJsonLd.map(r => complete(e.status, e.headers, r.json))
        case Right(v: Complete[A]) => IO.pure(complete(v.status, v.headers, v.value))
      }
      onSuccess(ioRoute.unsafeToFuture())(identity)
    }

  private[directives] type UseRight[A] = Either[Response[Unit], Complete[A]]

  implicit def ioEntityMarshaller[A: ToEntityMarshaller](
      io: IO[A]
  )(implicit cr: RemoteContextResolution, jo: JsonKeyOrdering): ResponseToMarshaller =
    ResponseToMarshaller(io.map[UseRight[A]](v => Right(Complete(OK, Seq.empty, None, v))))

  implicit def ioEntityMarshaller[E: JsonLdEncoder: HttpResponseFields, A: ToEntityMarshaller](
      io: IO[Either[E, A]]
  )(implicit cr: RemoteContextResolution, jo: JsonKeyOrdering): ResponseToMarshaller = {
    val ioComplete = io.map {
      _.bimap(
        e => Complete(e),
        a => Complete(OK, Seq.empty, None, a)
      )
    }
    ResponseToMarshaller(ioComplete)
  }

  implicit def ioResponseEntityMarshaller[E: JsonLdEncoder, A: ToEntityMarshaller](
      io: IO[Either[Response[E], A]]
  )(implicit cr: RemoteContextResolution, jo: JsonKeyOrdering): ResponseToMarshaller = {
    val ioComplete = io.map {
      _.map(a => Complete(OK, Seq.empty, None, a))
    }
    ResponseToMarshaller(ioComplete)
  }
}
