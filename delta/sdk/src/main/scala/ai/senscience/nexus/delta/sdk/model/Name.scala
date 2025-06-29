package ai.senscience.nexus.delta.sdk.model

import ai.senscience.nexus.delta.kernel.error.FormatError
import ai.senscience.nexus.delta.sdk.error.FormatErrors.IllegalNameFormatError
import cats.implicits.*
import io.circe.{Decoder, Encoder}
import pureconfig.ConfigReader
import pureconfig.error.CannotConvert

import scala.util.matching.Regex

/**
  * A valid name value that can be used to describe resources, like for example the display name of a realm.
  *
  * @param value
  *   the string representation of the name
  */
final case class Name private (value: String) extends AnyVal {
  override def toString: String = value
}

object Name {

  private[sdk] val regex: Regex = "[a-zA-Z0-9_\\-\\s]{1,128}".r

  /**
    * Attempts to construct a label from its string representation.
    *
    * @param value
    *   the string representation of the Label
    */
  def apply(value: String): Either[FormatError, Name] =
    value match {
      case regex() => Right(new Name(value))
      case _       => Left(IllegalNameFormatError())
    }

  /**
    * Constructs a Name from its string representation without validation in terms of allowed characters or size.
    *
    * @param value
    *   the string representation of the name
    */
  def unsafe(value: String): Name =
    new Name(value)

  implicit final val nameEncoder: Encoder[Name] =
    Encoder.encodeString.contramap(_.value)

  implicit final val nameDecoder: Decoder[Name] =
    Decoder.decodeString.emap(str => Name(str).leftMap(_.getMessage))

  implicit final val nameConfigReader: ConfigReader[Name] =
    ConfigReader.fromString(str =>
      Name(str).leftMap(err => CannotConvert(str, classOf[Name].getSimpleName, err.getMessage))
    )

}
