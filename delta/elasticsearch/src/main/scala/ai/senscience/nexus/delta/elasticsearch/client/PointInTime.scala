package ai.senscience.nexus.delta.elasticsearch.client

import io.circe.Codec
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveConfiguredCodec

final case class PointInTime(id: String)

object PointInTime {
  private given Configuration = Configuration.default
  given Codec[PointInTime]    = deriveConfiguredCodec[PointInTime]
}
