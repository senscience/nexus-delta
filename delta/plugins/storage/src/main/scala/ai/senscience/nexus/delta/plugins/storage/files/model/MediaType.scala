package ai.senscience.nexus.delta.plugins.storage.files.model

import cats.syntax.all.*
import io.circe.{Decoder, Encoder}
import org.apache.pekko.http.scaladsl.model.{ContentType, ContentTypes}
import org.http4s.MediaType as Http4sMediaType

final case class MediaType(mainType: String, subType: String) {
  def tree: String = s"$mainType/$subType"

  override def toString: String = tree
}

object MediaType {

  val `application/octet-stream`: MediaType = MediaType(Http4sMediaType.application.`octet-stream`)
  val `application/json`: MediaType         = MediaType(Http4sMediaType.application.json)
  val `text/plain`: MediaType               = MediaType(Http4sMediaType.text.plain)

  given Encoder[MediaType] = Encoder.encodeString.contramap(_.tree)

  given Decoder[MediaType] = Decoder.decodeString.emap(MediaType.parse)

  def apply(mt: Http4sMediaType): MediaType =
    MediaType(mt.mainType, mt.subType)

  def parse(str: String): Either[String, MediaType] =
    Http4sMediaType.parse(str).bimap(_.sanitized, apply)

  def toPekkoContentType(mt: MediaType): ContentType =
    ContentType.parse(mt.tree).getOrElse(ContentTypes.`application/octet-stream`)

}
