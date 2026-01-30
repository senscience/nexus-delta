package ai.senscience.nexus.delta.sourcing.model

import ai.senscience.nexus.delta.rdf.jsonld.ExpandedJsonLdCursor
import ai.senscience.nexus.delta.rdf.jsonld.decoder.JsonLdDecoder
import ai.senscience.nexus.delta.rdf.jsonld.decoder.JsonLdDecoderError.ParsingFailure
import ai.senscience.nexus.delta.sourcing.FragmentEncoder
import ai.senscience.nexus.delta.sourcing.model.Label.IllegalLabelFormat
import cats.implicits.*
import doobie.Put
import doobie.syntax.all.*
import doobie.util.Get
import io.circe.{Decoder, Encoder, KeyDecoder, KeyEncoder}

import scala.util.matching.Regex

sealed trait Tag extends Product with Serializable {

  def value: String

  override def toString: String = value

}

object Tag {

  sealed trait Latest extends Tag

  case object Latest extends Latest {
    override val value: String = "latest"

    given latestTagJsonLdDecoder: JsonLdDecoder[Latest] =
      (cursor: ExpandedJsonLdCursor) =>
        cursor.get[String].flatMap {
          case `value` => Right(Latest)
          case other   => Left(ParsingFailure(s"Tag '$other' does not match expected value 'latest'"))
        }

    given latestTagDecoder: Decoder[Latest] =
      Decoder.decodeString.emap(str => if str == "latest" then Right(Latest) else Left("Expected 'latest' string"))
  }

  val latest: Tag = Latest

  final case class UserTag private (value: String) extends Tag

  object UserTag {

    private[sourcing] val regex: Regex = "[\\p{ASCII}]{1,64}".r

    def unsafe(value: String) = new UserTag(value)

    def apply(value: String): Either[IllegalLabelFormat, UserTag] =
      Either.cond(value != "latest", value, IllegalLabelFormat("'latest' is a reserved tag")).flatMap {
        case value @ regex() => Right(new UserTag(value))
        case value           => Left(IllegalLabelFormat(value))
      }

    given Encoder[UserTag] = Encoder.encodeString.contramap(_.value)

    given userTagDecoder: Decoder[UserTag] = Decoder.decodeString.emap(str => UserTag(str).leftMap(_.message))

    given KeyEncoder[UserTag] = KeyEncoder.encodeKeyString.contramap(_.toString)

    given KeyDecoder[UserTag] = KeyDecoder.instance(UserTag(_).toOption)

    given userTagJsonLdDecoder: JsonLdDecoder[UserTag] = (cursor: ExpandedJsonLdCursor) =>
      cursor.get[String].flatMap { UserTag(_).leftMap { e => ParsingFailure(e.message) } }
  }

  given tagGet: Get[Tag] = Get[String].map {
    case "latest" => Latest
    case s        => UserTag.unsafe(s)
  }

  given tagPut: Put[Tag] = Put[String].contramap(_.value)

  given tagEncoder: Encoder[Tag] = Encoder.encodeString.contramap(_.value)

  given tagDecoder: Decoder[Tag] = Latest.latestTagDecoder.or(UserTag.userTagDecoder.map(identity[Tag]))

  given tagJsonLdDecoder: JsonLdDecoder[Tag] =
    Latest.latestTagJsonLdDecoder.or(UserTag.userTagJsonLdDecoder.covary[Tag])

  given tagFragmentEncoder: FragmentEncoder[Tag] = FragmentEncoder.instance { tag => Some(fr"tag = $tag") }
}
