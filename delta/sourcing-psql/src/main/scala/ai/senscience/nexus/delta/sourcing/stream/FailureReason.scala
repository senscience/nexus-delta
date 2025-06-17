package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.rdf.jsonld.context.JsonLdContext.keywords
import ch.epfl.bluebrain.nexus.delta.kernel.utils.ThrowableUtils.stackTraceAsString
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveConfiguredCodec
import io.circe.syntax.{EncoderOps, KeyOps}
import io.circe.{Codec, Encoder, Json}

import scala.util.control.NoStackTrace

case class FailureReason(`type`: String, value: Json) extends Exception with NoStackTrace

object FailureReason {

  implicit private val config: Configuration = Configuration.default.withDiscriminator(keywords.tpe)

  implicit val failureReasonCodec: Codec.AsObject[FailureReason] = deriveConfiguredCodec[FailureReason]

  def apply(throwable: Throwable): FailureReason =
    apply(throwable.getClass.getCanonicalName, throwable.getMessage, Some(stackTraceAsString(throwable)))

  def apply(errorType: String, message: String, stackTrace: Option[String]): FailureReason =
    FailureReason(
      "UnexpectedError",
      Json.obj(
        "message"    := message,
        "exception"  := errorType,
        "stacktrace" := stackTrace
      )
    )

  def apply[A: Encoder](tpe: String, value: A): FailureReason =
    FailureReason(tpe, value.asJson)

}
