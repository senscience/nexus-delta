package ai.senscience.nexus.pekko.marshalling

import io.circe.generic.semiauto.*
import io.circe.{Decoder, Encoder}

import java.time.Instant

final case class SimpleResource(id: String, rev: Int, createdAt: Instant, name: String, age: Int)
object SimpleResource {
  given Encoder.AsObject[SimpleResource] = deriveEncoder[SimpleResource]
  given Decoder[SimpleResource]          = deriveDecoder[SimpleResource]
}
