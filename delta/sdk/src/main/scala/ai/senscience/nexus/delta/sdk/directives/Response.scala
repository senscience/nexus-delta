package ai.senscience.nexus.delta.sdk.directives

import ai.senscience.nexus.delta.rdf.Vocabulary
import ai.senscience.nexus.delta.rdf.jsonld.context.JsonLdContext.keywords
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.error.SDKError
import ai.senscience.nexus.delta.sdk.marshalling.HttpResponseFields
import ai.senscience.nexus.delta.sdk.syntax.*
import cats.effect.IO
import io.circe.syntax.*
import io.circe.{Encoder, Json}
import org.apache.pekko.http.scaladsl.model.{HttpHeader, StatusCode}
import org.apache.pekko.http.scaladsl.server.{Rejection, Route}
import org.typelevel.otel4s.trace.Tracer

/**
  * An enumeration of possible Route responses
  */
sealed trait Response[A]

object Response {

  /**
    * An response that will be completed immediately
    */
  final case class Complete[A](
      status: StatusCode,
      headers: Seq[HttpHeader],
      entityTag: Option[String],
      value: A
  ) extends Response[A] {
    def map[B](f: A => B): Complete[B] = copy(value = f(value))
  }

  object Complete {

    /**
      * A constructor helper for when [[HttpResponseFields]] is present
      */
    def apply[A: HttpResponseFields](value: A): Complete[A] =
      Complete(value.status, value.headers, value.entityTag, value)
  }

  /**
    * A ''value'' that should be rejected
    */
  final case class Reject[E: JsonLdEncoder: Encoder: HttpResponseFields](error: E)
      extends SDKError
      with Response[E]
      with Rejection {

    /**
      * Generates a route that completes from the current rejection
      */
    def forceComplete(using RemoteContextResolution, JsonKeyOrdering, Tracer[IO]): Route =
      DeltaDirectives.discardEntityAndForceEmit(error)

    /**
      * @return
      *   the status code associated with this rejection
      */
    def status: StatusCode = error.status

    private[Response] def json: Json = error.asJson

    private def jsonValueWithStatus: Json = json.deepMerge(Json.obj("status" -> status.intValue().asJson))
  }

  object Reject {

    implicit final val seqRejectEncoder: Encoder[Seq[Reject[?]]] =
      Encoder.instance { rejections =>
        val rejectionsJson = Json.obj("rejections" -> rejections.map(_.jsonValueWithStatus).asJson)
        val tpe            = extractDistinctTypes(rejections) match {
          case head :: Nil => head
          case _           => "MultipleRejections"
        }
        rejectionsJson.deepMerge(Json.obj(keywords.tpe -> tpe.asJson))
      }

    private def extractDistinctTypes(rejections: Seq[Reject[?]]) =
      rejections.flatMap(_.json.hcursor.get[String](keywords.tpe).toOption).distinct

    implicit final val rejectJsonLdEncoder: JsonLdEncoder[Seq[Reject[?]]] =
      JsonLdEncoder.computeFromCirce(ContextValue(Vocabulary.contexts.error))
  }
}
