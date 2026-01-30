package ai.senscience.nexus.delta.sdk.model.routes

import ai.senscience.nexus.delta.sourcing.model.Tag.UserTag
import io.circe.Decoder
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveConfiguredDecoder

/**
  * The tag fields used as input/output on the routes
  *
  * @param rev
  *   the tag revision
  * @param tag
  *   the tag name
  */
final case class Tag(rev: Int, tag: UserTag)

object Tag {

  private given Configuration = Configuration.default.withStrictDecoding
  given Decoder[Tag]          = deriveConfiguredDecoder[Tag]

}
