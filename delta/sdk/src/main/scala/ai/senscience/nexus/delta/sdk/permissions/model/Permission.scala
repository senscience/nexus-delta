package ai.senscience.nexus.delta.sdk.permissions.model

import ai.senscience.nexus.delta.rdf.jsonld.ExpandedJsonLdCursor
import ai.senscience.nexus.delta.rdf.jsonld.decoder.JsonLdDecoder
import ai.senscience.nexus.delta.rdf.jsonld.decoder.JsonLdDecoderError.ParsingFailure
import ai.senscience.nexus.delta.sdk.error.FormatErrors.IllegalPermissionFormatError
import cats.Order
import cats.syntax.all.*
import io.circe.{Codec, Decoder, Encoder}

/**
  * Wraps a permission string that must begin with a letter, followed by at most 31 alphanumeric characters or symbols
  * among '-', '_', ':', '\' and '/'.
  *
  * @param value
  *   a valid permission string
  */
final case class Permission private (value: String) extends AnyVal {
  override def toString: String = value
}

object Permission {

  private[sdk] val regex = """[a-zA-Z][\w-:\\/]{0,31}""".r

  /**
    * Attempts to construct a [[Permission]] that passes the ''regex''
    *
    * @param value
    *   the permission value
    */
  final def apply(value: String): Either[IllegalPermissionFormatError, Permission] =
    value match {
      case regex() => Right(unsafe(value))
      case _       => Left(IllegalPermissionFormatError())
    }

  /**
    * Constructs a [[Permission]] without validating it against the ''regex''
    *
    * @param value
    *   the permission value
    */
  final def unsafe(value: String): Permission =
    new Permission(value)

  given Codec[Permission] =
    Codec.from(
      Decoder.decodeString.emap(str => Permission(str).leftMap(_.getMessage)),
      Encoder.encodeString.contramap(_.value)
    )

  given JsonLdDecoder[Permission] = (cursor: ExpandedJsonLdCursor) =>
    cursor
      .get[String]
      .flatMap(str => Permission(str).leftMap(_ => ParsingFailure("Json", str, cursor.history)))

  given Order[Permission] = Order.by(_.value)

}
