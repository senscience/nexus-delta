package ai.senscience.nexus.pekko.marshalling

import RdfMediaTypes.`application/ld+json`
import com.github.plokhotnyuk.jsoniter_scala.circe.JsoniterScalaCodec.*
import com.github.plokhotnyuk.jsoniter_scala.core.*
import io.circe.{Encoder, Json}
import org.apache.pekko.http.scaladsl.marshalling.{Marshaller, ToEntityMarshaller}
import org.apache.pekko.http.scaladsl.model.MediaTypes.`application/json`
import org.apache.pekko.http.scaladsl.model.{ContentType, HttpEntity, MediaType}
import org.apache.pekko.util.ByteString

trait CirceMarshalling {

  private val mediaTypes: Seq[MediaType.WithFixedCharset] = List(`application/json`, `application/ld+json`)

  private val defaultWriterConfig: WriterConfig = WriterConfig.withPreferredBufSize(100 * 1024)

  /**
    * `Json` => HTTP entity
    */
  given jsonMarshaller: ToEntityMarshaller[Json] =
    Marshaller.oneOf(mediaTypes*) { mediaType =>
      Marshaller.withFixedContentType(ContentType(mediaType)) { json =>
        HttpEntity(
          mediaType,
          ByteString(writeToArray(json, defaultWriterConfig))
        )
      }
    }

  /**
    * `A` => HTTP entity
    */
  given marshaller: [A: Encoder] => ToEntityMarshaller[A] =
    jsonMarshaller.compose(Encoder[A].apply)
}

object CirceMarshalling extends CirceMarshalling
