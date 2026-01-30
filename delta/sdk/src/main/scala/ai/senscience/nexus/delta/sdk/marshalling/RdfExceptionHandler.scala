package ai.senscience.nexus.delta.sdk.marshalling

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.kernel.utils.ClassUtils
import ai.senscience.nexus.delta.rdf.RdfError
import ai.senscience.nexus.delta.rdf.Vocabulary.contexts
import ai.senscience.nexus.delta.rdf.jsonld.context.JsonLdContext.keywords
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.*
import ai.senscience.nexus.delta.sdk.error.{AuthTokenError, IdentityError, ServiceError}
import ai.senscience.nexus.delta.sdk.jsonld.JsonLdRejection
import ai.senscience.nexus.delta.sdk.jws.JWSError
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.organizations.model.OrganizationRejection
import ai.senscience.nexus.delta.sdk.permissions.model.PermissionsRejection
import ai.senscience.nexus.delta.sdk.projects.model.ProjectRejection
import cats.effect.IO
import cats.effect.unsafe.implicits.*
import io.circe.syntax.*
import io.circe.{Encoder, JsonObject}
import org.apache.pekko.http.scaladsl.model.{EntityStreamSizeException, StatusCodes}
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.ExceptionHandler
import org.typelevel.otel4s.trace.Tracer

object RdfExceptionHandler {
  private val logger = Logger[RdfExceptionHandler.type]

  /**
    * An [[ExceptionHandler]] that returns RDF output (Json-LD compacted, Json-LD expanded, Dot or NTriples) depending
    * on content negotiation (Accept Header) and ''format'' query parameter
    */
  def apply(using BaseUri, RemoteContextResolution, JsonKeyOrdering, Tracer[IO]): ExceptionHandler =
    ExceptionHandler {
      case err: IdentityError             => discardEntityAndForceEmit(err)
      case err: JWSError                  => discardEntityAndForceEmit(err)
      case err: PermissionsRejection      => discardEntityAndForceEmit(err)
      case err: OrganizationRejection     => discardEntityAndForceEmit(err)
      case err: ProjectRejection          => discardEntityAndForceEmit(err)
      case err: JsonLdRejection           => discardEntityAndForceEmit(err)
      case err: AuthTokenError            => discardEntityAndForceEmit(err)
      case err: ServiceError              => discardEntityAndForceEmit(err)
      case err: RdfError                  => discardEntityAndForceEmit(err)
      case err: EntityStreamSizeException => discardEntityAndForceEmit(err)
      case err: Throwable                 =>
        onComplete(logger.error(err)("An exception was thrown while evaluating a Route'").unsafeToFuture()) { _ =>
          discardEntityAndForceEmit(UnexpectedError)
        }
    }

  private given Encoder[RdfError] =
    Encoder.AsObject.instance { r =>
      val tpe = ClassUtils.simpleName(r)
      JsonObject.empty.add(keywords.tpe, tpe.asJson).add("reason", r.reason.asJson)
    }

  private given JsonLdEncoder[RdfError] =
    JsonLdEncoder.computeFromCirce(ContextValue(contexts.error))

  private given HttpResponseFields[RdfError] =
    HttpResponseFields(_ => StatusCodes.InternalServerError)

  private given Encoder[EntityStreamSizeException] =
    Encoder.AsObject.instance { r =>
      val tpe    = "PayloadTooLarge"
      val reason = s"""Incoming payload size (${r.actualSize.getOrElse(
          "while streaming"
        )}) exceeded size limit (${r.limit} bytes)"""
      JsonObject(keywords.tpe -> tpe.asJson, "reason" -> reason.asJson)
    }

  private given JsonLdEncoder[EntityStreamSizeException] =
    JsonLdEncoder.computeFromCirce(ContextValue(contexts.error))

  private given HttpResponseFields[EntityStreamSizeException] =
    HttpResponseFields(_ => StatusCodes.ContentTooLarge)

  private case object UnexpectedError
  private type UnexpectedError = UnexpectedError.type

  private given Encoder[UnexpectedError] =
    Encoder.AsObject.instance { _ =>
      JsonObject(
        keywords.tpe -> "UnexpectedError".asJson,
        "reason"     -> "An unexpected error occurred. Please try again later or contact the administrator".asJson
      )
    }

  private given JsonLdEncoder[UnexpectedError] =
    JsonLdEncoder.computeFromCirce(ContextValue(contexts.error))

  private given HttpResponseFields[UnexpectedError] =
    HttpResponseFields(_ => StatusCodes.InternalServerError)

}
