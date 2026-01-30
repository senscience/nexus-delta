package ai.senscience.nexus.testkit

import io.circe.{parser, Json, JsonObject}

trait CirceLiteral {

  extension (sc: StringContext) {
    def json(args: Any*): Json =
      parser.parse(sc.s(args*)) match {
        case Right(value) => value
        case Left(err)    => throw new IllegalArgumentException(s"Failed to parse string into json. Details: '$err'")
      }

    def jobj(args: Any*): JsonObject = {
      val result = json(args*)
      result.asObject match {
        case Some(obj) => obj
        case None      => throw new IllegalArgumentException(s"Failed to convert to json object the json '$result'")
      }
    }
  }
}

object CirceLiteral extends CirceLiteral
