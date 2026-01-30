package ai.senscience.nexus.tests.iam.types

import io.circe.Decoder
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveConfiguredDecoder

final case class Permissions(permissions: Set[Permission], _rev: Int)

object Permissions {

  given Configuration = Configuration.default

  given Decoder[Permissions] = deriveConfiguredDecoder[Permissions]
}
