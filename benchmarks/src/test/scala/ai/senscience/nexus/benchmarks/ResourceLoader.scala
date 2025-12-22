package ai.senscience.nexus.benchmarks

import ai.senscience.nexus.delta.kernel.utils.FileUtils
import cats.effect.IO
import cats.effect.unsafe.implicits.*
import cats.syntax.all.*
import io.circe.Json
import fs2.io.file.Path

object ResourceLoader {

  def load(prefix: Path, file: String): IO[Json] = {
    val filePath = prefix.resolve(file)
    FileUtils.loadAsJson(filePath)
  }

  def loadSync(prefix: Path, file: String): Json =
    load(prefix, file).unsafeRunSync()

  def loadSync(prefix: Path, files: List[String]): List[Json] =
    files.parTraverse { load(prefix, _) }.unsafeRunSync()

}
