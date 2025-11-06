package ai.senscience.nexus.tests

import com.github.plokhotnyuk.jsoniter_scala.circe.JsoniterScalaCodec.*
import com.github.plokhotnyuk.jsoniter_scala.core.{writeToArray, WriterConfig}
import io.circe.Json
import org.apache.pekko.NotUsed
import org.apache.pekko.http.scaladsl.model.HttpHeader
import org.apache.pekko.http.scaladsl.model.headers.*
import org.apache.pekko.stream.scaladsl.{Compression, Source}
import org.apache.pekko.util.ByteString

object GzipCompression {

  def mustGzip(extraHeaders: Seq[HttpHeader]): Boolean =
    extraHeaders.exists {
      case `Content-Encoding`(encodings) => encodings.contains(HttpEncodings.gzip)
      case _                             => false
    }

  private val defaultWriterConfig: WriterConfig = WriterConfig.withPreferredBufSize(100 * 1024)

  def compress(json: Json): Source[ByteString, NotUsed] = {
    Source
      .single(ByteString(writeToArray(json, defaultWriterConfig)))
      .via(Compression.gzip)
  }

}
