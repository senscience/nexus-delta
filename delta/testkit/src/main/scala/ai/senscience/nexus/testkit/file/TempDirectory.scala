package ai.senscience.nexus.testkit.file

import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.effect.IO
import cats.effect.kernel.Resource
import fs2.{text, Stream}
import fs2.io.file.{Files, Path}
import io.circe.{Json, JsonObject}
import io.circe.syntax.EncoderOps
import munit.catseffect.IOFixture

object TempDirectory {

  def writeJson(directory: Path, fileName: String, content: JsonObject): IO[Path] =
    writeString(directory, fileName, content.asJson.noSpaces)

  def writeJson(directory: Path, fileName: String, content: Json): IO[Path] =
    writeString(directory, fileName, content.noSpaces)

  def writeString(directory: Path, fileName: String, content: String): IO[Path] = {
    val fullPath = directory / fileName
    Stream
      .emit(content)
      .through(text.utf8.encode)
      .through(Files[IO].writeAll(directory / fileName))
      .compile
      .drain
      .as(fullPath)
  }

  trait Fixture { self: NexusSuite =>
    val tempDirectory: IOFixture[Path] =
      ResourceSuiteLocalFixture(
        "tempDirectory",
        Resource.make(Files[IO].createTempDirectory)(Files[IO].deleteRecursively)
      )
  }

}
