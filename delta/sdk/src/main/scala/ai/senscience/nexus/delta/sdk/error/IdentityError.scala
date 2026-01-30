package ai.senscience.nexus.delta.sdk.error

import ai.senscience.nexus.delta.kernel.jwt.TokenRejection
import ai.senscience.nexus.delta.kernel.jwt.TokenRejection.InvalidAccessToken
import ai.senscience.nexus.delta.kernel.utils.ClassUtils
import ai.senscience.nexus.delta.rdf.IriOrBNode.BNode
import ai.senscience.nexus.delta.rdf.Vocabulary.contexts
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue
import ai.senscience.nexus.delta.rdf.jsonld.context.JsonLdContext.keywords
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.sdk.marshalling.HttpResponseFields
import ai.senscience.nexus.delta.sdk.syntax.*
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, JsonObject}
import org.apache.pekko.http.scaladsl.model.StatusCodes

/**
  * Top level error type that represents issues related to authentication and identities
  *
  * @param reason
  *   a human readable message for why the error occurred
  */
sealed abstract class IdentityError(reason: String) extends SDKError {

  override def getMessage: String = reason
}

object IdentityError {

  /**
    * Signals that the provided authentication is not valid.
    */
  case object AuthenticationFailed extends IdentityError("The supplied authentication is invalid.")

  /**
    * Signals an attempt to consume the service without a valid oauth2 bearer token.
    *
    * @param rejection
    *   the specific reason why the token is invalid
    */
  final case class InvalidToken(rejection: TokenRejection) extends IdentityError(rejection.getMessage)

  given Encoder.AsObject[TokenRejection] =
    Encoder.AsObject.instance { r =>
      val tpe  = ClassUtils.simpleName(r)
      val json = JsonObject.empty.add(keywords.tpe, tpe.asJson).add("reason", r.getMessage.asJson)
      r match {
        case InvalidAccessToken(_, _, error) => json.add("details", error.asJson)
        case _                               => json
      }
    }

  given JsonLdEncoder[TokenRejection] =
    JsonLdEncoder.computeFromCirce(id = BNode.random, ctx = ContextValue(contexts.error))

  given Encoder.AsObject[IdentityError] =
    Encoder.AsObject.instance[IdentityError] {
      case InvalidToken(r)      =>
        r.asJsonObject
      case AuthenticationFailed =>
        JsonObject(keywords.tpe -> "AuthenticationFailed".asJson, "reason" -> AuthenticationFailed.getMessage.asJson)
    }

  given JsonLdEncoder[IdentityError] = JsonLdEncoder.computeFromCirce(ContextValue(contexts.error))

  given HttpResponseFields[TokenRejection] =
    HttpResponseFields(_ => StatusCodes.Unauthorized)

  given HttpResponseFields[IdentityError] =
    HttpResponseFields {
      case IdentityError.AuthenticationFailed    => StatusCodes.Unauthorized
      case IdentityError.InvalidToken(rejection) => rejection.status
    }
}
