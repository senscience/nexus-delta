package ai.senscience.nexus.delta.sdk.realms.model

import ai.senscience.nexus.delta.sdk.instances.*
import ai.senscience.nexus.delta.sdk.model.Name
import cats.data.NonEmptySet
import io.circe.Decoder
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveConfiguredDecoder
import org.http4s.Uri
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader
import pureconfig.module.cats.*
import pureconfig.module.http4s.*

final case class RealmFields(
    name: Name,
    openIdConfig: Uri,
    logo: Option[Uri],
    acceptedAudiences: Option[NonEmptySet[String]]
)

object RealmFields {

  implicit final private val configuration: Configuration = Configuration.default.withStrictDecoding
  implicit val realmFieldsDecoder: Decoder[RealmFields]   = deriveConfiguredDecoder[RealmFields]

  implicit final val realmFieldsConfigReader: ConfigReader[RealmFields] = deriveReader[RealmFields]
}
