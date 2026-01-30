package ai.senscience.nexus.delta.sdk

import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sourcing.Serializer
import ai.senscience.nexus.delta.sourcing.implicits.CirceInstances.{read, write}
import ai.senscience.nexus.testkit.CirceLiteral
import ai.senscience.nexus.testkit.mu.{EitherAssertions, JsonAssertions, NexusSuite}
import ai.senscience.nexus.testkit.scalatest.{ClasspathResources, MUnitExtractValue}
import io.circe.{Json, JsonObject}
import munit.{Assertions, Location}

import scala.collection.immutable.VectorMap

abstract class SerializationSuite
    extends NexusSuite
    with Assertions
    with EitherAssertions
    with CirceLiteral
    with MUnitExtractValue
    with ClasspathResources
    with JsonAssertions
    with RemoteContextResolutionFixtures {

  given baseUri: BaseUri = BaseUri.unsafe("http://localhost", "v1")

  given res: RemoteContextResolution = loadCoreContextsAndSchemas

  def loadEvents[E](module: String, eventsToFile: (E, String)*): Map[E, (Json, JsonObject)] =
    eventsToFile.foldLeft(VectorMap.empty[E, (Json, JsonObject)]) { case (acc, (event, fileName)) =>
      acc + (event -> loadEvents(module, fileName))
    }

  def loadEvents(module: String, fileName: String): (Json, JsonObject) =
    (jsonContentOf(s"$module/database/$fileName"), jsonObjectContentOf(s"$module/sse/$fileName"))

  def loadDatabaseEvents(module: String, fileName: String): Json =
    jsonContentOf(s"$module/database/$fileName")

  private def generateOutput[Id, Value](serializer: Serializer[Id, Value], obtained: Value) =
    read(write(serializer.codec(obtained))(using serializer.jsonIterCodec))

  def assertOutput[Id, Value](serializer: Serializer[Id, Value], obtained: Value, expected: Json)(using
      Location
  ): Unit =
    assertEquals(
      generateOutput(serializer, obtained),
      expected
    )

  def assertOutputIgnoreOrder[Id, Value](serializer: Serializer[Id, Value], obtained: Value, expected: Json): Unit =
    generateOutput(serializer, obtained)
      .equalsIgnoreArrayOrder(expected)

}
