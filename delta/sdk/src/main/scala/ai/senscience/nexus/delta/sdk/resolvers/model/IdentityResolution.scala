package ai.senscience.nexus.delta.sdk.resolvers.model

import ai.senscience.nexus.delta.sourcing.Serializer
import ai.senscience.nexus.delta.sourcing.model.Identity
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveConfiguredCodec
import io.circe.syntax.*
import io.circe.{Codec, Encoder, Json, JsonObject}

/**
  * Enumeration of identity resolutions for a resolver
  */
sealed trait IdentityResolution

object IdentityResolution {

  /**
    * The resolution will use the identities of the caller at the moment of the resolution
    */
  case object UseCurrentCaller extends IdentityResolution

  /**
    * The resolution will rely on the provided entities
    * @param value
    *   the identities
    */
  final case class ProvidedIdentities(value: Set[Identity]) extends IdentityResolution

  implicit def identityResolutionEncoder(implicit
      identityEncoder: Encoder[Identity]
  ): Encoder.AsObject[IdentityResolution] = {
    Encoder.AsObject.instance {
      case UseCurrentCaller          => JsonObject.singleton("useCurrentCaller", Json.fromBoolean(true))
      case ProvidedIdentities(value) =>
        JsonObject.singleton("identities", value.asJson)
    }
  }

  object Database {
    import ai.senscience.nexus.delta.sourcing.model.Identity.Database.*
    implicit private val configuration: Configuration                        = Serializer.circeConfiguration
    implicit val identityResolutionCodec: Codec.AsObject[IdentityResolution] =
      deriveConfiguredCodec[IdentityResolution]
  }
}
