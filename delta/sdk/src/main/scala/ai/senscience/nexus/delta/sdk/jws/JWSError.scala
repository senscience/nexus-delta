package ai.senscience.nexus.delta.sdk.jws

import ai.senscience.nexus.delta.kernel.utils.ClassUtils
import ai.senscience.nexus.delta.rdf.IriOrBNode.BNode
import ai.senscience.nexus.delta.rdf.Vocabulary.contexts
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue
import ai.senscience.nexus.delta.rdf.jsonld.context.JsonLdContext.keywords
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.sdk.error.SDKError
import ai.senscience.nexus.delta.sdk.marshalling.HttpResponseFields
import io.circe.syntax.KeyOps
import io.circe.{Encoder, JsonObject}
import org.apache.pekko.http.scaladsl.model.StatusCodes

/**
  * Rejections related to JWS operations
  *
  * @param reason
  *   a descriptive message for reasons why the JWS operations failed
  */
sealed abstract class JWSError(val reason: String) extends SDKError {
  override def getMessage: String = reason
}

object JWSError {

  case object UnconfiguredJWS extends JWSError("JWS config is incorrect or missing. Please contact your administrator.")

  case object InvalidJWSPayload extends JWSError("Signature missing, flattened JWS format expected")

  case object JWSSignatureExpired extends JWSError("The payload expired")

  implicit val jwsErrorHttpResponseFields: HttpResponseFields[JWSError] = HttpResponseFields.fromStatusAndHeaders {
    case InvalidJWSPayload   => (StatusCodes.BadRequest, Seq.empty)
    case JWSSignatureExpired => (StatusCodes.Forbidden, Seq.empty)
    case UnconfiguredJWS     => (StatusCodes.InternalServerError, Seq.empty)
  }

  implicit val jwsErrorEncoder: Encoder.AsObject[JWSError] =
    Encoder.AsObject.instance { e =>
      val tpe = ClassUtils.simpleName(e)
      JsonObject(keywords.tpe := tpe, "reason" := e.reason)
    }

  implicit final val jwsErrorJsonLdEncoder: JsonLdEncoder[JWSError] =
    JsonLdEncoder.computeFromCirce(id = BNode.random, ctx = ContextValue(contexts.error))

}
