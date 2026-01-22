package ai.senscience.nexus.pekko.marshalling

import RdfMediaTypes.*
import com.github.plokhotnyuk.jsoniter_scala.circe.JsoniterScalaCodec.*
import com.github.plokhotnyuk.jsoniter_scala.core.*
import io.circe.{Decoder, Json, JsonObject}
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.model.ContentTypeRange
import org.apache.pekko.http.scaladsl.model.MediaTypes.`application/json`
import org.apache.pekko.http.scaladsl.server.Directive1
import org.apache.pekko.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshaller}
import org.apache.pekko.util.ByteString

import scala.concurrent.Future

/**
  * Unmarshallings that allow Pekko Http to convert an [[HttpEntity]] to a type ''A'' using a [[Decoder]] Partially
  * ported from ''de.heikoseeberger.akkahttpcirce.CirceSupport''.
  */
trait CirceUnmarshalling {

  private val unmarshallerContentTypes: Seq[ContentTypeRange] =
    List(`application/json`, `application/ld+json`, `application/sparql-results+json`).map(ContentTypeRange.apply)

  /**
    * HTTP entity => `Json`
    */
  given jsonUnmarshaller: FromEntityUnmarshaller[Json] =
    Unmarshaller.byteStringUnmarshaller
      .forContentTypes(unmarshallerContentTypes*)
      .map {
        case ByteString.empty => throw Unmarshaller.NoContentException
        case data             => readFromArray[Json](data.toArray)
      }

  val jsonEntity: Directive1[Json]             = entity(as[Json])
  val jsonObjectEntity: Directive1[JsonObject] = entity(as[JsonObject])

  /**
    * HTTP entity => `Json` => `A`
    */
  implicit final def decoderUnmarshaller[A: Decoder]: FromEntityUnmarshaller[A] =
    jsonUnmarshaller.map(Decoder[A].decodeJson).map(_.fold(throw _, identity))

  /**
    * ByteString => `Json`
    */
  implicit final def fromByteStringUnmarshaller[A: Decoder]: Unmarshaller[ByteString, A] =
    Unmarshaller[ByteString, Json](ec => bs => Future(readFromByteBuffer(bs.asByteBuffer))(ec))
      .map(Decoder[A].decodeJson)
      .map(_.fold(throw _, identity))
}

object CirceUnmarshalling extends CirceUnmarshalling
