package ai.senscience.nexus.delta.sdk.realms.model

import ai.senscience.nexus.delta.rdf.Vocabulary.{contexts, nxv}
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.sdk.OrderingFields
import ai.senscience.nexus.delta.sdk.model.Name
import ai.senscience.nexus.delta.sdk.realms.model.Realm.Metadata
import ai.senscience.nexus.delta.sourcing.model.Label
import cats.data.NonEmptySet
import io.circe.*
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveConfiguredEncoder
import org.http4s.Uri

/**
  * A realm representation.
  *
  * @param label
  *   the label of the realm
  * @param name
  *   the name of the realm
  * @param openIdConfig
  *   the address of the openid configuration
  * @param issuer
  *   an identifier for the issuer
  * @param grantTypes
  *   the supported grant types of the realm
  * @param logo
  *   an optional logo address
  * @param acceptedAudiences
  *   the optional set of audiences of this realm. JWT with `aud` which do not match this field will be rejected
  * @param authorizationEndpoint
  *   the authorization endpoint
  * @param tokenEndpoint
  *   the token endpoint
  * @param userInfoEndpoint
  *   the user info endpoint
  * @param revocationEndpoint
  *   an optional revocation endpoint
  * @param endSessionEndpoint
  *   an optional end session endpoint
  * @param keys
  *   the set of JWK keys as specified by rfc 7517 (https://tools.ietf.org/html/rfc7517)
  */
final case class Realm(
    label: Label,
    name: Name,
    openIdConfig: Uri,
    issuer: String,
    grantTypes: Set[GrantType],
    logo: Option[Uri],
    acceptedAudiences: Option[NonEmptySet[String]],
    authorizationEndpoint: Uri,
    tokenEndpoint: Uri,
    userInfoEndpoint: Uri,
    revocationEndpoint: Option[Uri],
    endSessionEndpoint: Option[Uri],
    keys: Set[Json]
) {

  /**
    * @return
    *   [[Realm]] metadata
    */
  def metadata: Metadata = Metadata(label)
}

object Realm {

  /**
    * Realm metadata.
    *
    * @param label
    *   the label of the realm
    */
  final case class Metadata(label: Label)

  import GrantType.Camel.given
  import ai.senscience.nexus.delta.sdk.instances.given

  private[Realm] given Configuration = Configuration.default.copy(transformMemberNames = {
    case "authorizationEndpoint" => nxv.authorizationEndpoint.prefix
    case "endSessionEndpoint"    => nxv.endSessionEndpoint.prefix
    case "grantTypes"            => nxv.grantTypes.prefix
    case "issuer"                => nxv.issuer.prefix
    case "label"                 => nxv.label.prefix
    case "revocationEndpoint"    => nxv.revocationEndpoint.prefix
    case "tokenEndpoint"         => nxv.tokenEndpoint.prefix
    case "userInfoEndpoint"      => nxv.userInfoEndpoint.prefix
    case other                   => other
  })

  given Encoder.AsObject[Realm] =
    deriveConfiguredEncoder[Realm].mapJsonObject(
      _.remove("keys")
    )

  val context: ContextValue  = ContextValue(contexts.realms)
  given JsonLdEncoder[Realm] = JsonLdEncoder.computeFromCirce(context)

  private given Encoder.AsObject[Metadata] = deriveConfiguredEncoder[Metadata]
  given JsonLdEncoder[Metadata]            = JsonLdEncoder.computeFromCirce(ContextValue(contexts.realmsMetadata))

  given OrderingFields[Realm] =
    OrderingFields {
      case "_label"  => Ordering[String] on (_.label.value)
      case "_issuer" => Ordering[String] on (_.issuer)
    }
}
