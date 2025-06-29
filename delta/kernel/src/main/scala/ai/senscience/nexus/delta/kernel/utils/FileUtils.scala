package ai.senscience.nexus.delta.kernel.utils

import ai.senscience.nexus.delta.kernel.error.LoadFileError.{InvalidJson, UnaccessibleFile}
import cats.effect.IO
import cats.syntax.all.*
import io.circe.parser.{decode, parse}
import io.circe.{Decoder, Json}

import java.nio.file.{Files, Path}

object FileUtils {

  /**
    * Extracts the extension from the given filename
    */
  def extension(filename: String): Option[String] = {
    val lastDotIndex = filename.lastIndexOf('.')
    Option.when(lastDotIndex >= 0) {
      filename.substring(lastDotIndex + 1)
    }
  }

  def filenameWithoutExtension(filename: String): Option[String] = {
    val lastDotIndex = filename.lastIndexOf('.')
    Option.when(lastDotIndex > 0) {
      filename.substring(0, lastDotIndex)
    }
  }

  /**
    * Load the content of the given file as a string
    */
  def loadAsString(filePath: Path): IO[String] =
    IO.blocking(Files.readString(filePath))
      .adaptError { case th => UnaccessibleFile(filePath, th) }

  def loadAsJson(filePath: Path): IO[Json] =
    loadAsString(filePath).flatMap { content =>
      IO.fromEither(parse(content))
        .adaptError { e => InvalidJson(filePath, e.getMessage) }
    }

  /**
    * Load the content of the given file as json and try to decode it as an A
    * @param filePath
    *   the path of the target file
    */
  def loadJsonAs[A: Decoder](filePath: Path): IO[A] =
    for {
      content <- loadAsString(filePath)
      json    <- IO.fromEither(decode[A](content).leftMap { e => InvalidJson(filePath, e.getMessage) })
    } yield json

}
