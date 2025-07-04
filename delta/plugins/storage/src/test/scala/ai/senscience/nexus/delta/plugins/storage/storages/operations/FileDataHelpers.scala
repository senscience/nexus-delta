package ai.senscience.nexus.delta.plugins.storage.storages.operations

import ai.senscience.nexus.delta.sdk.FileData
import cats.effect.IO
import fs2.Stream

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

trait FileDataHelpers {

  def streamData(data: String): FileData = {
    val bytes = data.getBytes(StandardCharsets.UTF_8)
    Stream.emit(ByteBuffer.wrap(bytes))
  }

  def consume(data: FileData): IO[String] =
    data.compile.lastOrError.map { buffer =>
      StandardCharsets.UTF_8.decode(buffer).toString
    }

}
