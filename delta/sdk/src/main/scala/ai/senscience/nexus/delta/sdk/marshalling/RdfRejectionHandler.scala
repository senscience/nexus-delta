package ai.senscience.nexus.delta.sdk.marshalling

import ai.senscience.nexus.delta.kernel.utils.ClassUtils
import ai.senscience.nexus.delta.rdf.IriOrBNode.BNode
import ai.senscience.nexus.delta.rdf.Vocabulary.contexts
import ai.senscience.nexus.delta.rdf.jsonld.context.JsonLdContext.keywords
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.*
import ai.senscience.nexus.delta.sdk.directives.Response.Reject
import ai.senscience.nexus.delta.sdk.syntax.*
import cats.effect.IO
import io.circe.syntax.*
import io.circe.{DecodingFailure, Encoder, JsonObject}
import org.apache.pekko.http.scaladsl.model.headers.*
import org.apache.pekko.http.scaladsl.model.{ContentRange, EntityStreamSizeException, StatusCodes}
import org.apache.pekko.http.scaladsl.server.*
import org.apache.pekko.http.scaladsl.server.AuthenticationFailedRejection.{CredentialsMissing, CredentialsRejected}
import org.typelevel.otel4s.trace.Tracer

// $COVERAGE-OFF$
@SuppressWarnings(Array("UnsafeTraversableMethods"))
object RdfRejectionHandler {

  /**
    * Adapted from [[org.apache.pekko.http.scaladsl.server.RejectionHandler.default]] A [[RejectionHandler]] that
    * returns RDF output (Json-LD compacted, Json-LD expanded, Dot or NTriples) depending on content negotiation (Accept
    * Header) and ''format'' query parameter
    */
  def apply(using RemoteContextResolution, JsonKeyOrdering, Tracer[IO]): RejectionHandler =
    RejectionHandler
      .newBuilder()
      .handleAll[SchemeRejection] { rejections => discardEntityAndForceEmit(rejections) }
      .handleAll[MethodRejection] { rejections => discardEntityAndForceEmit(rejections) }
      .handle { case AuthorizationFailedRejection => discardEntityAndForceEmit(AuthorizationFailedRejection) }
      .handle { case r: MalformedFormFieldRejection => discardEntityAndForceEmit(r) }
      .handle { case r: MalformedHeaderRejection => discardEntityAndForceEmit(r) }
      .handle { case r: MalformedQueryParamRejection => discardEntityAndForceEmit(r) }
      .handle { case r: MalformedRequestContentRejection => discardEntityAndForceEmit(r) }
      .handle { case r: MissingCookieRejection => discardEntityAndForceEmit(r) }
      .handle { case r: MissingFormFieldRejection => discardEntityAndForceEmit(r) }
      .handle { case r: MissingHeaderRejection => discardEntityAndForceEmit(r) }
      .handle { case r: MissingAttributeRejection[?] => discardEntityAndForceEmit(r) }
      .handle { case r: InvalidOriginRejection => discardEntityAndForceEmit(r) }
      .handle { case r: MissingQueryParamRejection => discardEntityAndForceEmit(r) }
      .handle { case r: InvalidRequiredValueForQueryParamRejection => discardEntityAndForceEmit(r) }
      .handle { case RequestEntityExpectedRejection => discardEntityAndForceEmit(RequestEntityExpectedRejection) }
      .handle { case r: TooManyRangesRejection => discardEntityAndForceEmit(r) }
      .handle { case r: CircuitBreakerOpenRejection => discardEntityAndForceEmit(r) }
      .handle { case r: UnsatisfiableRangeRejection => discardEntityAndForceEmit(r) }
      .handleAll[Reject[?]] {
        case Seq(head)                => head.forceComplete
        case multiple @ Seq(head, _*) => discardEntityAndForceEmit(head.status, multiple)
        case _                        => discardEntityAndForceEmit(StatusCodes.InternalServerError, RejectionExpected)
      }
      .handleAll[AuthenticationFailedRejection] { rejections => discardEntityAndForceEmit(rejections) }
      .handleAll[UnacceptedResponseContentTypeRejection] { discardEntityAndForceEmit(_) }
      .handleAll[UnacceptedResponseEncodingRejection] { discardEntityAndForceEmit(_) }
      .handleAll[UnsupportedRequestContentTypeRejection] { discardEntityAndForceEmit(_) }
      .handleAll[UnsupportedRequestEncodingRejection] { discardEntityAndForceEmit(_) }
      .handle { case ExpectedWebSocketRequestRejection => discardEntityAndForceEmit(ExpectedWebSocketRequestRejection) }
      .handleAll[UnsupportedWebSocketSubprotocolRejection] { discardEntityAndForceEmit(_) }
      .handle { case r: ValidationRejection => discardEntityAndForceEmit(r) }
      .handle { case ResourceNotFound => discardEntityAndForceEmit(StatusCodes.NotFound, ResourceNotFound) }
      .handleNotFound { discardEntityAndForceEmit(StatusCodes.NotFound, ResourceNotFound) }
      .result()
      .withFallback(RejectionHandler.default)

  private val bnode = BNode.random

  private[marshalling] given Encoder.AsObject[SchemeRejection] =
    Encoder.AsObject.instance { rejection =>
      val msg = s"Uri scheme not allowed, supported schemes: ${rejection.supported}"
      jsonObj(rejection, msg)
    }

  private[marshalling] given Encoder.AsObject[Seq[SchemeRejection]] =
    Encoder.AsObject.instance { rejections =>
      val msg = s"Uri scheme not allowed, supported schemes: ${rejections.map(_.supported).sorted.mkString(", ")}"
      jsonObj(rejections.head, msg)
    }

  private given HttpResponseFields[SchemeRejection] =
    HttpResponseFields(_ => StatusCodes.BadRequest)

  private given HttpResponseFields[Seq[SchemeRejection]] =
    HttpResponseFields(_ => StatusCodes.BadRequest)

  private[marshalling] given methodRejectionEncoder: Encoder.AsObject[MethodRejection] =
    Encoder.AsObject.instance { rejection =>
      jsonObj(
        rejection,
        s"HTTP method not allowed, supported methods: ${rejection.supported.name}.",
        tpe = Some("HttpMethodNotAllowed")
      )
    }

  private[marshalling] given methodsRejectionEncoder: Encoder.AsObject[Seq[MethodRejection]] =
    Encoder.AsObject.instance { rejections =>
      val names = rejections.map(_.supported.name).sorted.mkString(", ")
      jsonObj(
        rejections.head,
        s"HTTP method not allowed, supported methods: $names.",
        tpe = Some("HttpMethodNotAllowed")
      )
    }

  private given methodRejectionResponseFields: HttpResponseFields[MethodRejection] =
    HttpResponseFields.fromStatusAndHeaders(r => StatusCodes.MethodNotAllowed -> Seq(Allow(r.supported)))

  private given methodSeqRejectionResponseFields: HttpResponseFields[Seq[MethodRejection]] =
    HttpResponseFields.fromStatusAndHeaders(r => StatusCodes.MethodNotAllowed -> Seq(Allow(r.map(r => r.supported))))

  private[marshalling] given authFailedRejectionEncoder: Encoder.AsObject[AuthenticationFailedRejection] =
    Encoder.AsObject.instance { rejection =>
      val rejectionMessage = rejection.cause match {
        case CredentialsMissing  => "The resource requires authentication, which was not supplied with the request."
        case CredentialsRejected => "The supplied authentication is invalid."
      }
      jsonObj(rejection, rejectionMessage)
    }

  private[marshalling] given authSeqFailedRejectionEncoder: Encoder.AsObject[Seq[AuthenticationFailedRejection]] =
    Encoder.AsObject.instance { rejections =>
      val rejectionMessage = rejections.head.cause match {
        case CredentialsMissing  => "The resource requires authentication, which was not supplied with the request."
        case CredentialsRejected => "The supplied authentication is invalid."
      }
      jsonObj(rejections.head, rejectionMessage)
    }

  private given authFailedRejectionResponseFields: HttpResponseFields[AuthenticationFailedRejection] =
    HttpResponseFields.fromStatusAndHeaders(r => StatusCodes.Unauthorized -> Seq(`WWW-Authenticate`(r.challenge)))

  private given authFailedRejectionSeqResponseFields: HttpResponseFields[Seq[AuthenticationFailedRejection]] =
    HttpResponseFields.fromStatusAndHeaders(r =>
      StatusCodes.Unauthorized -> r.map(r => `WWW-Authenticate`(r.challenge))
    )

  private[marshalling] given unacceptedResponseEncEncoder: Encoder.AsObject[UnacceptedResponseEncodingRejection] =
    Encoder.AsObject.instance { rejection =>
      val supported = rejection.supported.map(_.value).toList.sorted.mkString(", ")
      val msg       = s"Resource representation is only available with these Content-Encodings: $supported."
      jsonObj(rejection, msg)
    }

  private[marshalling] given unacceptedResponseEncSeqEncoder
      : Encoder.AsObject[Seq[UnacceptedResponseEncodingRejection]] =
    Encoder.AsObject.instance { rejections =>
      val supported = rejections.flatMap(_.supported).map(_.value).sorted.mkString(", ")
      val msg       = s"Resource representation is only available with these Content-Encodings: $supported."
      jsonObj(rejections.head, msg)
    }

  private given unacceptedResponseFields: HttpResponseFields[UnacceptedResponseEncodingRejection] =
    HttpResponseFields(_ => StatusCodes.NotAcceptable)

  private given unacceptedSeqResponseFields: HttpResponseFields[Seq[UnacceptedResponseEncodingRejection]] =
    HttpResponseFields(_ => StatusCodes.NotAcceptable)

  private[marshalling] given unsupportedRequestEncEncoder: Encoder.AsObject[UnsupportedRequestEncodingRejection] =
    Encoder.AsObject.instance { rejection =>
      val supported = rejection.supported.value.mkString(" or ")
      jsonObj(rejection, s"The request's Content-Encoding is not supported. Expected: $supported")
    }

  private[marshalling] given unsupportedRequestEncSeqEncoder
      : Encoder.AsObject[Seq[UnsupportedRequestEncodingRejection]] =
    Encoder.AsObject.instance { rejections =>
      val supported = rejections.map(_.supported.value).sorted.mkString(" or ")
      jsonObj(rejections.head, s"The request's Content-Encoding is not supported. Expected: $supported")
    }

  private[marshalling] given unsupportedRequestEncResponseFields
      : HttpResponseFields[UnsupportedRequestEncodingRejection] =
    HttpResponseFields(_ => StatusCodes.BadRequest)

  private[marshalling] given unsupportedRequestEncSeqResponseFields
      : HttpResponseFields[Seq[UnsupportedRequestEncodingRejection]] =
    HttpResponseFields(_ => StatusCodes.BadRequest)

  private[marshalling] given unsupportedReqCtEncoder: Encoder.AsObject[UnsupportedRequestContentTypeRejection] =
    Encoder.AsObject.instance { rejection =>
      val unsupported = rejection.contentType.fold("")(_.toString)
      val supported   = rejection.supported.mkString(" or ")
      val expected    = if supported.isEmpty then "" else s" Expected: $supported"
      jsonObj(rejection, s"The request's Content-Type $unsupported is not supported.$expected")
    }

  private[marshalling] given unsupportedReqCtSeqEncoder: Encoder.AsObject[Seq[UnsupportedRequestContentTypeRejection]] =
    Encoder.AsObject.instance { rejections =>
      val unsupported = rejections.find(_.contentType.isDefined).flatMap(_.contentType).fold("")(" [" + _ + "]")
      val supported   = rejections.flatMap(_.supported).mkString(" or ")
      val expected    = if supported.isEmpty then "" else s" Expected: $supported"
      jsonObj(rejections.head, s"The request's Content-Type $unsupported is not supported.$expected")
    }

  private given unsupportedReqCtResponseFields: HttpResponseFields[UnsupportedRequestContentTypeRejection] =
    HttpResponseFields(_ => StatusCodes.UnsupportedMediaType)

  private given unsupportedReqCtSeqResponseFields: HttpResponseFields[Seq[UnsupportedRequestContentTypeRejection]] =
    HttpResponseFields(_ => StatusCodes.UnsupportedMediaType)

  given unacceptedResponseCtEncoder: Encoder.AsObject[UnacceptedResponseContentTypeRejection] =
    Encoder.AsObject.instance { rejection =>
      val supported = rejection.supported.map(_.format).toList.sorted.mkString(", ")
      val msg       = s"Resource representation is only available with these types: '$supported'"
      jsonObj(rejection, msg)
    }

  private[marshalling] given unacceptedResponseCtSeqEncoder
      : Encoder.AsObject[Seq[UnacceptedResponseContentTypeRejection]] =
    Encoder.AsObject.instance { rejections =>
      val supported = rejections.flatMap(_.supported).map(_.format).toList.sorted.mkString(", ")
      val msg       = s"Resource representation is only available with these types: '$supported'"
      jsonObj(rejections.head, msg)
    }

  given unacceptedResponseCtFields: HttpResponseFields[UnacceptedResponseContentTypeRejection] =
    HttpResponseFields(_ => StatusCodes.NotAcceptable)

  private given unacceptedResponseCtSeqFields: HttpResponseFields[Seq[UnacceptedResponseContentTypeRejection]] =
    HttpResponseFields(_ => StatusCodes.NotAcceptable)

  private[marshalling] given unsupportedWSProtoEncoder: Encoder.AsObject[UnsupportedWebSocketSubprotocolRejection] =
    Encoder.AsObject.instance { rejection =>
      val supported = rejection.supportedProtocol
      val msg       = s"None of the websocket subprotocols offered in the request are supported. Supported are $supported."
      jsonObj(rejection, msg)
    }

  private[marshalling] given unsupportedWSProtoSeqEncoder
      : Encoder.AsObject[Seq[UnsupportedWebSocketSubprotocolRejection]] =
    Encoder.AsObject.instance { rejections =>
      val supported = rejections.map(_.supportedProtocol).sorted.mkString(",")
      val msg       =
        s"None of the websocket subprotocols offered in the request are supported. Supported are $supported."
      jsonObj(rejections.head, msg)
    }

  private given unsupportedWSProtoFields: HttpResponseFields[UnsupportedWebSocketSubprotocolRejection] =
    HttpResponseFields.fromStatusAndHeaders(r =>
      (StatusCodes.BadRequest, Seq(new RawHeader("Sec-WebSocket-Protocol", r.supportedProtocol)))
    )

  private given unsupportedWSProtoSeqFields: HttpResponseFields[Seq[UnsupportedWebSocketSubprotocolRejection]] =
    HttpResponseFields.fromStatusAndHeaders { r =>
      val supported = r.map(_.supportedProtocol).sorted.mkString(", ")
      (StatusCodes.BadRequest, Seq(new RawHeader("Sec-WebSocket-Protocol", supported)))
    }

  private[marshalling] given Encoder.AsObject[AuthorizationFailedRejection.type] =
    Encoder.AsObject.instance { rejection =>
      jsonObj(rejection, "The supplied authentication is not authorized to access this resource.")
    }

  private given HttpResponseFields[AuthorizationFailedRejection.type] =
    HttpResponseFields(_ => StatusCodes.Forbidden)

  private[marshalling] given Encoder.AsObject[MalformedFormFieldRejection] =
    Encoder.AsObject.instance { case r @ MalformedFormFieldRejection(name, msg, _) =>
      jsonObj(r, s"The form field '$name' was malformed.", Some(msg))
    }

  private given HttpResponseFields[MalformedFormFieldRejection] =
    HttpResponseFields(_ => StatusCodes.BadRequest)

  private[marshalling] given Encoder.AsObject[MalformedHeaderRejection] =
    Encoder.AsObject.instance { case r @ MalformedHeaderRejection(headerName, msg, _) =>
      jsonObj(r, s"The value of HTTP header '$headerName' was malformed.", Some(msg))
    }

  private given HttpResponseFields[MalformedHeaderRejection] =
    HttpResponseFields(_ => StatusCodes.BadRequest)

  given Encoder.AsObject[MalformedQueryParamRejection] =
    Encoder.AsObject.instance { case r @ MalformedQueryParamRejection(name, msg, _) =>
      jsonObj(r, s"The query parameter '$name' was malformed.", Some(msg))
    }

  given HttpResponseFields[MalformedQueryParamRejection] =
    HttpResponseFields(_ => StatusCodes.BadRequest)

  private[marshalling] given Encoder.AsObject[MalformedRequestContentRejection] =
    Encoder.AsObject.instance {
      case r @ MalformedRequestContentRejection(_, EntityStreamSizeException(limit, _)) =>
        jsonObj(r, s"The request payload exceed the maximum configured limit '$limit'.")
      case r @ MalformedRequestContentRejection(_, f: DecodingFailure)                  =>
        val details = Option.when(f.getMessage() != "DecodingFailure at : JSON decoding to CNil should never happen")(
          f.getMessage()
        )
        jsonObj(r, "The request content was malformed.", details)
      case r @ MalformedRequestContentRejection(msg, _)                                 =>
        jsonObj(r, "The request content was malformed.", Some(msg))
    }

  private given HttpResponseFields[MalformedRequestContentRejection] =
    HttpResponseFields {
      case MalformedRequestContentRejection(_, EntityStreamSizeException(_, _)) => StatusCodes.ContentTooLarge
      case _                                                                    => StatusCodes.BadRequest
    }

  private[marshalling] given Encoder.AsObject[MissingCookieRejection] =
    Encoder.AsObject.instance { case r @ MissingCookieRejection(cookieName) =>
      jsonObj(r, s"Request is missing required cookie '$cookieName'.")
    }

  private given HttpResponseFields[MissingCookieRejection] =
    HttpResponseFields(_ => StatusCodes.BadRequest)

  private[marshalling] given Encoder.AsObject[MissingFormFieldRejection] =
    Encoder.AsObject.instance { case r @ MissingFormFieldRejection(fieldName) =>
      jsonObj(r, s"Request is missing required form field '$fieldName'.")
    }

  private given HttpResponseFields[MissingFormFieldRejection] =
    HttpResponseFields(_ => StatusCodes.BadRequest)

  private[marshalling] given Encoder.AsObject[MissingHeaderRejection] =
    Encoder.AsObject.instance { case r @ MissingHeaderRejection(headerName) =>
      jsonObj(r, s"Request is missing required HTTP header '$headerName'.")
    }

  private given HttpResponseFields[MissingHeaderRejection] =
    HttpResponseFields(_ => StatusCodes.BadRequest)

  private[marshalling] given Encoder.AsObject[InvalidOriginRejection] =
    Encoder.AsObject.instance { case r @ InvalidOriginRejection(allowedOrigins) =>
      jsonObj(r, s"Allowed `Origin` header values: ${allowedOrigins.mkString(", ")}")
    }

  private given HttpResponseFields[InvalidOriginRejection] =
    HttpResponseFields(_ => StatusCodes.Forbidden)

  private[marshalling] given Encoder.AsObject[MissingQueryParamRejection] =
    Encoder.AsObject.instance { case r @ MissingQueryParamRejection(paramName) =>
      jsonObj(r, s"Request is missing required query parameter '$paramName'.", tpe = Some("MissingQueryParam"))
    }

  private given HttpResponseFields[MissingQueryParamRejection] =
    HttpResponseFields(_ => StatusCodes.BadRequest)

  private[marshalling] given Encoder.AsObject[InvalidRequiredValueForQueryParamRejection] =
    Encoder.AsObject.instance { case r @ InvalidRequiredValueForQueryParamRejection(paramName, requiredValue, _) =>
      jsonObj(r, s"Request is missing required value '$requiredValue' for query parameter '$paramName'")
    }

  private given HttpResponseFields[InvalidRequiredValueForQueryParamRejection] =
    HttpResponseFields(_ => StatusCodes.NotFound)

  private[marshalling] given Encoder.AsObject[RequestEntityExpectedRejection.type] =
    Encoder.AsObject.instance { case r @ RequestEntityExpectedRejection =>
      jsonObj(r, "Request entity expected but not supplied")
    }

  private given HttpResponseFields[RequestEntityExpectedRejection.type] =
    HttpResponseFields(_ => StatusCodes.BadRequest)

  private[marshalling] given [A] => Encoder.AsObject[MissingAttributeRejection[A]] =
    Encoder.AsObject.instance { case r @ MissingAttributeRejection(_) =>
      jsonObj(r, StatusCodes.InternalServerError.defaultMessage)
    }

  private given [A] => HttpResponseFields[MissingAttributeRejection[A]] =
    HttpResponseFields(_ => StatusCodes.InternalServerError)

  private[marshalling] given Encoder.AsObject[TooManyRangesRejection] =
    Encoder.AsObject.instance { r => jsonObj(r, "Request contains too many ranges") }

  private given HttpResponseFields[TooManyRangesRejection] =
    HttpResponseFields(_ => StatusCodes.RangeNotSatisfiable)

  private[marshalling] given Encoder.AsObject[CircuitBreakerOpenRejection] =
    Encoder.AsObject.instance { r => jsonObj(r, "") }

  private given HttpResponseFields[CircuitBreakerOpenRejection] =
    HttpResponseFields(_ => StatusCodes.ServiceUnavailable)

  private[marshalling] given Encoder.AsObject[UnsatisfiableRangeRejection] =
    Encoder.AsObject.instance { case r @ UnsatisfiableRangeRejection(unsatisfiableRanges, _) =>
      val reason =
        s"None of the following requested Ranges were satisfiable: '${unsatisfiableRanges.mkString(", ")}'"
      jsonObj(r, reason)
    }

  private given HttpResponseFields[UnsatisfiableRangeRejection] =
    HttpResponseFields.fromStatusAndHeaders { r =>
      (StatusCodes.RangeNotSatisfiable, Seq(`Content-Range`(ContentRange.Unsatisfiable(r.actualEntityLength))))
    }

  private given Encoder.AsObject[ExpectedWebSocketRequestRejection.type] =
    Encoder.AsObject.instance { case r @ ExpectedWebSocketRequestRejection =>
      jsonObj(r, "Expected WebSocket Upgrade request")
    }

  private given HttpResponseFields[ExpectedWebSocketRequestRejection.type] =
    HttpResponseFields(_ => StatusCodes.BadRequest)

  private[marshalling] given Encoder.AsObject[ValidationRejection] =
    Encoder.AsObject.instance { case r @ ValidationRejection(msg, _) =>
      jsonObj(r, msg)
    }

  private given HttpResponseFields[ValidationRejection] =
    HttpResponseFields(_ => StatusCodes.BadRequest)

  given compactFromCirceRejection: [A <: Rejection: Encoder.AsObject] => JsonLdEncoder[A] =
    JsonLdEncoder.computeFromCirce(id = bnode, ctx = ContextValue(contexts.error))

  given compactFromCirceRejectionSeq: [A <: Seq[Rejection]: Encoder.AsObject] => JsonLdEncoder[A] =
    JsonLdEncoder.computeFromCirce(id = bnode, ctx = ContextValue(contexts.error))

  private def jsonObj[A <: Rejection](
      value: A,
      reason: String,
      details: Option[String] = None,
      tpe: Option[String] = None
  ): JsonObject =
    JsonObject.fromIterable(
      List(keywords.tpe -> tpe.getOrElse(ClassUtils.simpleName(value)).asJson) ++
        Option.when(reason.trim.nonEmpty)("reason" -> reason.asJson) ++
        details.collect { case d if d.trim.nonEmpty => "details" -> d.asJson }
    )

  /**
    * A resource endpoint cannot be found on the platform
    */
  case object ResourceNotFound extends Rejection {

    type ResourceNotFound = ResourceNotFound.type

    private[marshalling] val resourceNotFoundJson =
      JsonObject(keywords.tpe -> "ResourceNotFound".asJson, "reason" -> "The requested resource does not exist.".asJson)

    private[marshalling] given Encoder.AsObject[ResourceNotFound] =
      Encoder.AsObject.instance(_ => resourceNotFoundJson)

    private[marshalling] given JsonLdEncoder[ResourceNotFound] =
      JsonLdEncoder.computeFromCirce(id = BNode.random, ctx = ContextValue(contexts.error))
  }

  case object RejectionExpected extends Rejection {

    private type RejectionExpected = RejectionExpected.type

    private[marshalling] val rejectionRejectedJson =
      JsonObject(keywords.tpe -> "RejectionExpected".asJson, "reason" -> "At least one rejection was expected.".asJson)

    private[marshalling] given Encoder.AsObject[RejectionExpected] =
      Encoder.AsObject.instance(_ => rejectionRejectedJson)

    private[marshalling] given JsonLdEncoder[RejectionExpected] =
      JsonLdEncoder.computeFromCirce(id = BNode.random, ctx = ContextValue(contexts.error))
  }

  object all {
    given Encoder.AsObject[Rejection]   = Encoder.AsObject.instance {
      case ResourceNotFound                              => RdfRejectionHandler.ResourceNotFound.resourceNotFoundJson
      case r: MethodRejection                            => r.asJsonObject
      case r: SchemeRejection                            => r.asJsonObject
      case AuthorizationFailedRejection                  => AuthorizationFailedRejection.asJsonObject
      case r: MalformedFormFieldRejection                => r.asJsonObject
      case r: MalformedHeaderRejection                   => r.asJsonObject
      case r: MalformedQueryParamRejection               => r.asJsonObject
      case r: ValidationRejection                        => r.asJsonObject
      case r: MissingAttributeRejection[?]               => r.asJsonObject
      case RequestEntityExpectedRejection                => RequestEntityExpectedRejection.asJsonObject
      case ExpectedWebSocketRequestRejection             => ExpectedWebSocketRequestRejection.asJsonObject
      case r: TooManyRangesRejection                     => r.asJsonObject
      case r: CircuitBreakerOpenRejection                => r.asJsonObject
      case r: MissingCookieRejection                     => r.asJsonObject
      case r: MissingHeaderRejection                     => r.asJsonObject
      case r: MissingFormFieldRejection                  => r.asJsonObject
      case r: InvalidOriginRejection                     => r.asJsonObject
      case r: MissingQueryParamRejection                 => r.asJsonObject
      case r: UnsupportedRequestContentTypeRejection     => r.asJsonObject
      case r: UnacceptedResponseEncodingRejection        => r.asJsonObject
      case r: UnsupportedRequestEncodingRejection        => r.asJsonObject
      case r: AuthenticationFailedRejection              => r.asJsonObject
      case r: MalformedRequestContentRejection           => r.asJsonObject
      case r: UnacceptedResponseContentTypeRejection     => r.asJsonObject
      case r: UnsupportedWebSocketSubprotocolRejection   => r.asJsonObject
      case r: UnsatisfiableRangeRejection                => r.asJsonObject
      case r: InvalidRequiredValueForQueryParamRejection => r.asJsonObject
      case r: Rejection                                  => jsonObj(r, r.toString)
    }
    // format: off
    given HttpResponseFields[Rejection] = HttpResponseFields.fromStatusAndHeaders {
      case ResourceNotFound                              => (StatusCodes.NotFound, Seq.empty)
      case r: MethodRejection                            => (r.status, r.headers)
      case r: SchemeRejection                            => (r.status, r.headers)
      case AuthorizationFailedRejection                  => (AuthorizationFailedRejection.status, AuthorizationFailedRejection.headers)
      case r: MalformedFormFieldRejection                => (r.status, r.headers)
      case r: MalformedHeaderRejection                   => (r.status, r.headers)
      case r: MalformedQueryParamRejection               => (r.status, r.headers)
      case r: ValidationRejection                        => (r.status, r.headers)
      case r: MissingAttributeRejection[?]               => (r.status, r.headers)
      case RequestEntityExpectedRejection                => (RequestEntityExpectedRejection.status, RequestEntityExpectedRejection.headers)
      case ExpectedWebSocketRequestRejection             => (ExpectedWebSocketRequestRejection.status, ExpectedWebSocketRequestRejection.headers)
      case r: TooManyRangesRejection                     => (r.status, r.headers)
      case r: CircuitBreakerOpenRejection                => (r.status, r.headers)
      case r: MissingCookieRejection                     => (r.status, r.headers)
      case r: MissingHeaderRejection                     => (r.status, r.headers)
      case r: MissingFormFieldRejection                  => (r.status, r.headers)
      case r: InvalidOriginRejection                     => (r.status, r.headers)
      case r: MissingQueryParamRejection                 => (r.status, r.headers)
      case r: UnsupportedRequestContentTypeRejection     => (r.status, r.headers)
      case r: UnacceptedResponseEncodingRejection        => (r.status, r.headers)
      case r: UnsupportedRequestEncodingRejection        => (r.status, r.headers)
      case r: AuthenticationFailedRejection              => (r.status, r.headers)
      case r: MalformedRequestContentRejection           => (r.status, r.headers)
      case r: UnacceptedResponseContentTypeRejection     => (r.status, r.headers)
      case r: UnsupportedWebSocketSubprotocolRejection   => (r.status, r.headers)
      case r: UnsatisfiableRangeRejection                => (r.status, r.headers)
      case r: InvalidRequiredValueForQueryParamRejection => (r.status, r.headers)
      case _: Rejection                                  => (StatusCodes.BadRequest, Seq.empty)
    }
    // format: on
  }
}
// $COVERAGE-ON$
