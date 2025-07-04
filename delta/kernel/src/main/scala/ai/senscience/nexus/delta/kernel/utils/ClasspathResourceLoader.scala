package ai.senscience.nexus.delta.kernel.utils

import ai.senscience.nexus.delta.kernel.utils.ClasspathResourceError.{InvalidJson, InvalidJsonObject, ResourcePathNotFound}
import cats.effect.{IO, Resource}
import fs2.text
import io.circe.parser.parse
import io.circe.{Json, JsonObject}

import java.io.{IOException, InputStream}

class ClasspathResourceLoader private (classLoader: ClassLoader) {

  final def absolutePath(resourcePath: String): IO[String] = {
    IO.blocking(
      Option(classLoader.getResource(resourcePath))
        .toRight(ResourcePathNotFound(resourcePath))
    ).rethrow
      .map(_.getPath)
  }

  /**
    * Loads the content of the argument classpath resource as an [[InputStream]].
    *
    * @param resourcePath
    *   the path of a resource available on the classpath
    * @return
    *   the content of the referenced resource as an [[InputStream]] or a [[ClasspathResourceError]] when the resource
    *   is not found
    */
  def streamOf(resourcePath: String): Resource[IO, InputStream] = {
    Resource.fromAutoCloseable(
      IO.blocking {
        Option(classLoader.getResourceAsStream(resourcePath))
          .toRight(new IOException(s"Resource '$resourcePath' not found"))
      }.rethrow
    )
  }

  /**
    * Loads the content of the argument classpath resource as a string and replaces all the key matches of the
    * ''replacements'' with their values.
    *
    * @param resourcePath
    *   the path of a resource available on the classpath
    * @return
    *   the content of the referenced resource as a string or a [[ClasspathResourceError]] when the resource is not
    *   found
    */
  final def contentOf(
      resourcePath: String,
      attributes: (String, Any)*
  ): IO[String] =
    resourceAsTextFrom(resourcePath).map(Handlebars(_, attributes.toMap))

  /**
    * Loads the content of the argument classpath resource as a string and replaces all the key matches of the
    * ''replacements'' with their values. The resulting string is parsed into a json value.
    *
    * @param resourcePath
    *   the path of a resource available on the classpath
    * @return
    *   the content of the referenced resource as a json value or an [[ClasspathResourceError]] when the resource is not
    *   found or is not a Json
    */
  final def jsonContentOf(
      resourcePath: String,
      attributes: (String, Any)*
  ): IO[Json] =
    for {
      text <- contentOf(resourcePath, attributes*)
      json <- IO.fromEither(parse(text).left.map(InvalidJson(resourcePath, text, _)))
    } yield json

  /**
    * Loads the content of the argument classpath resource as a string and replaces all the key matches of the
    * ''replacements'' with their values. The resulting string is parsed into a json object.
    *
    * @param resourcePath
    *   the path of a resource available on the classpath
    * @return
    *   the content of the referenced resource as a json value or an [[ClasspathResourceError]] when the resource is not
    *   found or is not a Json
    */
  final def jsonObjectContentOf(resourcePath: String, attributes: (String, Any)*): IO[JsonObject] =
    for {
      json    <- jsonContentOf(resourcePath, attributes*)
      jsonObj <- IO.fromOption(json.asObject)(InvalidJsonObject(resourcePath))
    } yield jsonObj

  private def resourceAsTextFrom(resourcePath: String): IO[String] = {
    fs2.io
      .readClassLoaderResource[IO](resourcePath, classLoader = classLoader)
      .through(text.utf8.decode)
      .compile
      .string
  }
}

object ClasspathResourceLoader {

  /**
    * Creates a resource loader using the standard ClassLoader
    */
  def apply(): ClasspathResourceLoader = new ClasspathResourceLoader(getClass.getClassLoader)

  /**
    * Creates a resource loader using the ClassLoader of the argument class.
    *
    * This is necessary when files on the classpath are located in modules from a plugin. Otherwise, prefer using the
    * standard ClasspathResourceLoader.
    */
  def withContext(`class`: Class[?]): ClasspathResourceLoader = new ClasspathResourceLoader(
    `class`.getClassLoader
  )
}
