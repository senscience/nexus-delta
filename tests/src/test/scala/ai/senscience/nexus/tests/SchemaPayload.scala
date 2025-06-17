package ai.senscience.nexus.tests

import ai.senscience.nexus.delta.kernel.utils.ClasspathResourceLoader
import cats.effect.IO
import io.circe.Json

object SchemaPayload {
  private val loader = ClasspathResourceLoader()

  def loadSimple(targetClass: String = "schema:TestResource"): IO[Json] =
    loader.jsonContentOf("kg/schemas/simple-schema.json", "targetClass" -> targetClass)

  def loadSimpleNoId(targetClass: String = "schema:TestResource"): IO[Json] =
    loader.jsonContentOf("kg/schemas/simple-schema-no-id.json", "targetClass" -> targetClass)

}
