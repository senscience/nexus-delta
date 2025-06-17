package ai.senscience.nexus.akka.marshalling

import io.circe.generic.semiauto.*
import io.circe.{Decoder, Encoder}

import java.time.Instant

final case class SimpleResource(id: String, rev: Int, createdAt: Instant, name: String, age: Int)
object SimpleResource {
  implicit val simpleResourceEncoder: Encoder.AsObject[SimpleResource] = deriveEncoder[SimpleResource]
  implicit val simpleResourceDecoder: Decoder[SimpleResource]          = deriveDecoder[SimpleResource]
}
