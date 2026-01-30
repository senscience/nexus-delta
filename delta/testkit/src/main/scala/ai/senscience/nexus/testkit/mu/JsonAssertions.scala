package ai.senscience.nexus.testkit.mu

import io.circe.syntax.EncoderOps
import io.circe.{Json, JsonObject, Printer}
import munit.{Assertions, Location}

trait JsonAssertions { self: Assertions =>

  private given jsonPrinter: Printer = Printer.spaces2

  extension (json: Json) {

    private def sortKeys(value: Json): Json = {
      def canonicalJson(json: Json): Json =
        json.arrayOrObject[Json](
          json,
          arr => Json.fromValues(arr.map(canonicalJson).sortBy(_.hashCode)),
          obj => sorted(obj).asJson
        )

      def sorted(jObj: JsonObject): JsonObject =
        JsonObject.fromIterable(jObj.toVector.sortBy(_._1).map { case (k, v) => k -> canonicalJson(v) })

      canonicalJson(value)
    }

    def equalsIgnoreArrayOrder(expected: Json)(using Location): Unit = {
      val obtainedSorted = sortKeys(json)
      val expectedSorted = sortKeys(expected)
      assertEquals(
        obtainedSorted,
        expectedSorted,
        s"Both Json are not equal (ignoring array order)\n${jsonPrinter
            .print(obtainedSorted)}\ndid not equal\n${jsonPrinter.print(expectedSorted)}"
      )
    }
  }
}
