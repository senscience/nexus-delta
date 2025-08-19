package ai.senscience.nexus.delta.sdk.directives

import ai.senscience.nexus.akka.marshalling.RdfMediaTypes.*
import ai.senscience.nexus.delta.sdk.fusion.FusionConfig
import ai.senscience.nexus.delta.sdk.marshalling.JsonLdFormat
import ai.senscience.nexus.delta.sdk.model.IdSegmentRef
import ai.senscience.nexus.delta.sdk.model.IdSegmentRef.{Latest, Revision, Tag}
import ai.senscience.nexus.delta.sdk.utils.HeadersUtils
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.offset.Offset
import akka.http.scaladsl.coding.Coders
import akka.http.scaladsl.model.*
import akka.http.scaladsl.model.MediaTypes.{`application/json`, `text/html`}
import akka.http.scaladsl.model.StatusCodes.{Redirection, SeeOther}
import akka.http.scaladsl.model.headers.*
import akka.http.scaladsl.server.*
import akka.http.scaladsl.server.ContentNegotiator.Alternative
import akka.http.scaladsl.server.Directives.*
import cats.effect.IO
import org.http4s.Uri

object DeltaDirectives extends DeltaDirectives

trait DeltaDirectives extends UriDirectives {

  // order is important
  val mediaTypes: List[MediaType.WithFixedCharset] =
    List(
      `application/ld+json`,
      `application/json`,
      `application/n-triples`,
      `application/n-quads`,
      `text/vnd.graphviz`
    )

  private val fusionRange: MediaRange.One = MediaRange.One(`text/html`, 1f)

  /**
    * Completes the current Route with the provided conversion to any available entity marshaller
    */
  def emit(response: ResponseToMarshaller): Route =
    response(None)

  /**
    * Completes the current Route with the provided conversion to any available entity marshaller
    */
  def emit(status: StatusCode, response: ResponseToMarshaller): Route =
    response(Some(status))

  /**
    * Completes the current Route with the provided conversion to json
    */
  def emitJson(response: ResponseToJson): Route = response()

  /**
    * Completes the current Route with the provided conversion to original payloads
    */
  def emit(response: ResponseToOriginalSource): Route = response()

  /**
    * Completes the current Route with the provided conversion to SSEs
    */
  def emit(response: ResponseToSse): Route = response()

  /**
    * Completes the current Route with the provided conversion to Json-LD
    */
  def emit(status: StatusCode, response: ResponseToJsonLd): Route =
    response(Some(status))

  /**
    * Completes the current Route with the provided conversion to Json-LD
    */
  def emit(response: ResponseToJsonLd): Route =
    response(None)

  /**
    * Completes the current Route with the provided redirection and conversion to Json-LD in case of an error.
    */
  def emitRedirect(redirection: Redirection, response: ResponseToRedirect): Route =
    response(redirection)

  /**
    * Completes the current Route discarding the entity and completing with the provided conversion to Json-LD. If the
    * Json-LD cannot be be completed for any reason, it returns the plain Json representation
    */
  def discardEntityAndForceEmit(response: ResponseToJsonLdDiscardingEntity): Route =
    response(None)

  /**
    * Completes the current Route discarding the entity and completing with the provided status code and conversion to
    * Json-LD. If the Json-LD cannot be be completed for any reason, it returns the plain Json representation
    */
  def discardEntityAndForceEmit(status: StatusCode, response: ResponseToJsonLdDiscardingEntity): Route =
    response(Some(status))

  def unacceptedMediaTypeRejection(values: Seq[MediaType]): UnacceptedResponseContentTypeRejection =
    UnacceptedResponseContentTypeRejection(values.map(mt => Alternative(mt)).toSet)

  def requestMediaType: Directive1[MediaType] =
    extractRequest.flatMap { req =>
      HeadersUtils.findFirst(req.headers, mediaTypes) match {
        case Some(value) => provide(value)
        case None        => reject(unacceptedMediaTypeRejection(mediaTypes))
      }
    }

  /**
    * Returns the best of the given encoding alternatives given the preferences the client indicated in the request's
    * `Accept-Encoding` headers.
    *
    * This implementation is based on the akka internal implemetation in
    * `akka.http.scaladsl.server.directives.CodingDirectives#_encodeResponse`
    */
  def requestEncoding: Directive1[HttpEncoding] =
    extractRequest.map { request =>
      val negotiator                    = EncodingNegotiator(request.headers)
      val encoders                      = Seq(Coders.NoCoding, Coders.Gzip, Coders.Deflate)
      val encodings: List[HttpEncoding] = encoders.map(_.encoding).toList
      negotiator
        .pickEncoding(encodings)
        .flatMap(be => encoders.find(_.encoding == be))
        .map(_.encoding)
        .getOrElse(HttpEncodings.identity)
    }

  def conditionalCache(value: Option[String], mediaType: MediaType, encoding: HttpEncoding): Directive0 =
    conditionalCache(value, mediaType, None, encoding)

  /**
    * Wraps its inner route with support for Conditional Requests as defined by http://tools.ietf.org/html/rfc7232
    *
    * Supports `Etag` header:
    * https://doc.akka.io/docs/akka-http/10.0/routing-dsl/directives/cache-condition-directives/conditional.html
    */
  def conditionalCache(
      value: Option[String],
      mediaType: MediaType,
      jsonldFormat: Option[JsonLdFormat],
      encoding: HttpEncoding
  ): Directive0 = {
    val entityTag = value.map(EtagUtils.compute(_, mediaType, jsonldFormat, encoding))
    Directives.conditional(entityTag, None)
  }

  /**
    * If the `Accept` header is set to `text/html`, redirect to the matching resource page in fusion if the feature is
    * enabled
    */
  def emitOrFusionRedirect(project: ProjectRef, id: IdSegmentRef, emitDelta: Route)(implicit
      config: FusionConfig
  ): Route = {
    val resourceBase =
      config.base / project.organization.value / project.project.value / "resources" / id.value.asString
    emitOrFusionRedirect(
      id match {
        case _: Latest        => resourceBase
        case Revision(_, rev) => resourceBase.withQueryParam("rev", rev.toString)
        case Tag(_, tag)      => resourceBase.withQueryParam("tag", tag.value)
      },
      emitDelta
    )
  }

  /**
    * If the `Accept` header is set to `text/html`, redirect to the matching project page in fusion if the feature is
    * enabled
    */
  def emitOrFusionRedirect(project: ProjectRef, emitDelta: Route)(implicit
      config: FusionConfig
  ): Route =
    emitOrFusionRedirect(
      config.base / "admin" / project.organization.value / project.project.value,
      emitDelta
    )

  def emitOrFusionRedirect(fusionUri: org.http4s.Uri, emitDelta: Route)(implicit config: FusionConfig): Route =
    extractRequest { req =>
      if (config.enableRedirects && req.header[Accept].exists(_.mediaRanges.contains(fusionRange))) {
        emitRedirect(SeeOther, IO.pure(fusionUri))
      } else
        emitDelta
    }

  /**
    * Extracts an [[Offset]] value from the ''Last-Event-ID'' header, defaulting to [[Offset.Start]]. An invalid value
    * will result in an [[MalformedHeaderRejection]].
    */
  def lastEventId: Directive1[Offset] =
    optionalHeaderValueByName(`Last-Event-ID`.name).map(_.map(id => `Last-Event-ID`(id))).flatMap {
      case Some(value) =>
        value.id.toLongOption match {
          case None    =>
            val msg =
              s"Invalid '${`Last-Event-ID`.name}' header value '${value.id}', expected a Long value."
            reject(MalformedHeaderRejection(`Last-Event-ID`.name, msg))
          case Some(o) => provide(Offset.at(o))
        }
      case None        => provide(Offset.Start)
    }

  /** Injects a `Vary: Accept,Accept-Encoding` into the response */
  def varyAcceptHeaders: Directive0 =
    vary(Set(Accept.name, `Accept-Encoding`.name))

  /** The URI of fusion's id resolution endpoint */
  def fusionResolveUri(id: Uri)(implicit config: FusionConfig): Uri =
    config.base / "resolve" / id.toString

  private def vary(headers: Set[String]): Directive0 =
    respondWithHeader(RawHeader("Vary", headers.mkString(",")))

  private def respondWithHeader(responseHeader: HttpHeader): Directive0 =
    mapSuccessResponse(r => r.withHeaders(r.headers :+ responseHeader))

  private def mapSuccessResponse(f: HttpResponse => HttpResponse): Directive0 =
    mapRouteResultPF {
      case RouteResult.Complete(response) if response.status.isSuccess => RouteResult.Complete(f(response))
    }
}
