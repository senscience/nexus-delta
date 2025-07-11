package ai.senscience.nexus.delta.sdk.directives

import ai.senscience.nexus.akka.marshalling.RdfMediaTypes.*
import ai.senscience.nexus.delta.rdf.jsonld.api.{JsonLdApi, TitaniumJsonLdApi}
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.JsonLdValue
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.*
import ai.senscience.nexus.delta.sdk.directives.Response.Complete
import ai.senscience.nexus.delta.sdk.marshalling.JsonLdFormat.{Compacted, Expanded}
import ai.senscience.nexus.delta.sdk.marshalling.RdfMarshalling.*
import ai.senscience.nexus.delta.sdk.marshalling.{HttpResponseFields, JsonLdFormat}
import ai.senscience.nexus.delta.sdk.syntax.*
import akka.http.scaladsl.marshalling.ToEntityMarshaller
import akka.http.scaladsl.model.*
import akka.http.scaladsl.model.MediaTypes.`application/json`
import akka.http.scaladsl.model.StatusCodes.OK
import akka.http.scaladsl.model.headers.{Accept, HttpEncoding, RawHeader}
import akka.http.scaladsl.server.Directives.*
import akka.http.scaladsl.server.Route
import cats.effect.IO
import cats.effect.unsafe.implicits.*

import java.nio.charset.StandardCharsets
import java.util.Base64

sealed trait ResponseToJsonLd {
  def apply(statusOverride: Option[StatusCode]): Route
}

object ResponseToJsonLd extends FileBytesInstances {

  def apply(
      io: IO[Complete[JsonLdValue]]
  )(implicit cr: RemoteContextResolution, jo: JsonKeyOrdering): ResponseToJsonLd =
    new ResponseToJsonLd {

      // Some resources may not have been created in the system with a strict configuration
      // (and if they are, there is no need to check them again)
      implicit private val api: JsonLdApi = TitaniumJsonLdApi.lenient

      override def apply(statusOverride: Option[StatusCode]): Route = {

        val ioFinal = statusOverride.fold(io) { status => io.map { _.copy(status = status) } }

        def marshaller[R: ToEntityMarshaller](
            handle: JsonLdValue => IO[R],
            mediaType: MediaType,
            jsonldFormat: Option[JsonLdFormat],
            encoding: HttpEncoding
        ): Route = {
          val ioRoute = ioFinal.flatMap { case Complete(status, headers, entityTag, value) =>
            handle(value).map { r =>
              conditionalCache(entityTag, mediaType, jsonldFormat, encoding) {
                complete(status, headers, r)
              }
            }
          }
          onSuccess(ioRoute.unsafeToFuture())(identity)
        }

        requestEncoding { encoding =>
          requestMediaType {
            case mediaType if mediaType == `application/ld+json` =>
              jsonLdFormatOrReject {
                case Expanded  => marshaller(v => v.encoder.expand(v.value), mediaType, Some(Expanded), encoding)
                case Compacted => marshaller(v => v.encoder.compact(v.value), mediaType, Some(Compacted), encoding)
              }

            case mediaType if mediaType == `application/json` =>
              jsonLdFormatOrReject {
                case Expanded  =>
                  marshaller(v => v.encoder.expand(v.value).map(_.json), mediaType, Some(Expanded), encoding)
                case Compacted =>
                  marshaller(v => v.encoder.compact(v.value).map(_.json), mediaType, Some(Compacted), encoding)
              }

            case mediaType if mediaType == `application/n-triples` =>
              marshaller(v => v.encoder.ntriples(v.value), mediaType, None, encoding)

            case mediaType if mediaType == `application/n-quads` =>
              marshaller(v => v.encoder.nquads(v.value), mediaType, None, encoding)

            case mediaType if mediaType == `text/vnd.graphviz` =>
              marshaller(v => v.encoder.dot(v.value), mediaType, None, encoding)

            case _ => reject(unacceptedMediaTypeRejection(mediaTypes))
          }
        }
      }
    }

  def fromComplete[A: JsonLdEncoder](
      io: IO[Complete[A]]
  )(implicit cr: RemoteContextResolution, jo: JsonKeyOrdering): ResponseToJsonLd =
    apply(io.map { c => c.map(JsonLdValue(_)) })

  def fromFile(
      io: IO[FileResponse]
  )(implicit jo: JsonKeyOrdering, cr: RemoteContextResolution): ResponseToJsonLd =
    new ResponseToJsonLd {

      // From the RFC 2047: "=?" charset "?" encoding "?" encoded-text "?="
      private def attachmentString(filename: String): String = {
        val encodedFilename = Base64.getEncoder.encodeToString(filename.getBytes(StandardCharsets.UTF_8))
        s"=?UTF-8?B?$encodedFilename?="
      }

      override def apply(statusOverride: Option[StatusCode]): Route = {
        val flattened = io.flatMap { fr =>
          fr.content.map {
            _.map { s =>
              fr.metadata -> s
            }
          }
        }

        onSuccess(flattened.unsafeToFuture()) {
          case Left(c)                    => emit(c)
          case Right((metadata, content)) =>
            headerValueByType(Accept) { accept =>
              if (accept.mediaRanges.exists(_.matches(metadata.contentType.mediaType))) {
                val encodedFilename    = attachmentString(metadata.filename)
                val contentDisposition =
                  RawHeader("Content-Disposition", s"""attachment; filename="$encodedFilename"""")
                requestEncoding { encoding =>
                  conditionalCache(metadata.entityTag, metadata.contentType.mediaType, encoding) {
                    respondWithHeaders(contentDisposition, metadata.headers*) {
                      complete(statusOverride.getOrElse(OK), HttpEntity(metadata.contentType, content))
                    }
                  }
                }

              } else
                reject(unacceptedMediaTypeRejection(Seq(metadata.contentType.mediaType)))
            }
        }
      }
    }
}

sealed trait FileBytesInstances extends ValueInstances {

  implicit def ioFileBytes(
      io: IO[FileResponse]
  )(implicit jo: JsonKeyOrdering, cr: RemoteContextResolution): ResponseToJsonLd =
    ResponseToJsonLd.fromFile(io)

  implicit def fileBytesValue(
      value: FileResponse
  )(implicit jo: JsonKeyOrdering, cr: RemoteContextResolution): ResponseToJsonLd =
    ResponseToJsonLd.fromFile(IO.pure(value))

}

sealed trait ValueInstances extends LowPriorityValueInstances {

  implicit def ioValue[A: JsonLdEncoder: HttpResponseFields](
      io: IO[A]
  )(implicit cr: RemoteContextResolution, jo: JsonKeyOrdering): ResponseToJsonLd =
    ResponseToJsonLd.fromComplete(io.map(v => Complete(v)))

  implicit def ioJsonLdValue(
      io: IO[JsonLdValue]
  )(implicit cr: RemoteContextResolution, jo: JsonKeyOrdering): ResponseToJsonLd =
    ResponseToJsonLd(
      io.map { value =>
        Complete(OK, Seq.empty, None, value)
      }
    )

  implicit def completeValue[A: JsonLdEncoder](
      value: Complete[A]
  )(implicit cr: RemoteContextResolution, jo: JsonKeyOrdering): ResponseToJsonLd =
    ResponseToJsonLd.fromComplete(IO.pure(value))

  implicit def valueWithHttpResponseFields[A: JsonLdEncoder: HttpResponseFields](
      value: A
  )(implicit cr: RemoteContextResolution, jo: JsonKeyOrdering): ResponseToJsonLd =
    ResponseToJsonLd.fromComplete(IO.pure(Complete(value)))
}

sealed trait LowPriorityValueInstances {
  implicit def valueWithoutHttpResponseFields[A: JsonLdEncoder](
      value: A
  )(implicit cr: RemoteContextResolution, jo: JsonKeyOrdering): ResponseToJsonLd =
    ResponseToJsonLd.fromComplete(IO.pure(Complete(OK, Seq.empty, None, value)))
}
