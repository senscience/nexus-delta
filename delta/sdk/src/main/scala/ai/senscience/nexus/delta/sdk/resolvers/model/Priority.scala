package ai.senscience.nexus.delta.sdk.resolvers.model

import ai.senscience.nexus.delta.rdf.jsonld.ExpandedJsonLdCursor
import ai.senscience.nexus.delta.rdf.jsonld.decoder.JsonLdDecoder
import ai.senscience.nexus.delta.rdf.jsonld.decoder.JsonLdDecoderError.ParsingFailure
import ai.senscience.nexus.delta.sdk.error.FormatErrors.ResolverPriorityIntervalError
import cats.implicits.*
import io.circe.{Decoder, Encoder}

/**
  * A safe representation of a resolver priority
  */
final case class Priority private (value: Int) extends AnyVal {
  override def toString: String = value.toString
}

object Priority {

  private val min = 0
  private val max = 1000

  /**
    * Attempts to get a priority from an integer
    */
  def apply(value: Int): Either[ResolverPriorityIntervalError, Priority] =
    Either.cond(
      value >= min && value <= max,
      new Priority(value),
      ResolverPriorityIntervalError(value, min, max)
    )

  /**
    * Construct a priority from an integer without validation
    */
  def unsafe(value: Int) = new Priority(value)

  implicit val priorityEncoder: Encoder[Priority] = Encoder.encodeInt.contramap(_.value)
  implicit val priorityDecoder: Decoder[Priority] = Decoder.decodeInt.emap(Priority(_).leftMap(_.getMessage))

  implicit val priorityJsonLdDecoder: JsonLdDecoder[Priority] =
    (cursor: ExpandedJsonLdCursor) =>
      cursor.get[Int].flatMap { Priority(_).leftMap { e => ParsingFailure(e.getMessage) } }
}
