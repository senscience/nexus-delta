package ai.senscience.nexus.delta.sourcing.model

import doobie.{Get, Put}
import io.circe.{Decoder, Encoder, KeyEncoder}

/**
  * Entity type
  */
final case class EntityType(value: String) extends AnyVal {
  override def toString: String = value
}

object EntityType {
  given Get[EntityType] = Get[String].map(EntityType(_))
  given Put[EntityType] = Put[String].contramap(_.value)

  given Encoder[EntityType] = Encoder.encodeString.contramap(_.value)
  given Decoder[EntityType] = Decoder.decodeString.map(EntityType(_))

  given KeyEncoder[EntityType] = KeyEncoder.encodeKeyString.contramap(_.toString)
}
