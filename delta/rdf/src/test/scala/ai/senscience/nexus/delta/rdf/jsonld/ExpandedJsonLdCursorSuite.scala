package ai.senscience.nexus.delta.rdf.jsonld

import ai.senscience.nexus.delta.rdf.RdfLoader
import ai.senscience.nexus.delta.rdf.Vocabulary.schema
import ai.senscience.nexus.delta.rdf.jsonld.decoder.JsonLdDecoderError.ParsingFailure.KeyMissingFailure
import ai.senscience.nexus.delta.rdf.jsonld.decoder.JsonLdDecoderError.{DecodingFailure, ParsingFailure}
import ai.senscience.nexus.testkit.mu.NexusSuite
import io.circe.CursorOp.{DownArray, DownField}

class ExpandedJsonLdCursorSuite extends NexusSuite with RdfLoader {

  private def loadCursor = expanded("jsonld/decoder/cocktail.json").map(_.cursor)

  private val drinks      = schema + "drinks"
  private val alcohol     = schema + "alcohol"
  private val ingredients = schema + "ingredients"
  private val name        = schema + "name"
  private val steps       = schema + "steps"
  private val volume      = schema + "volume"
  private val value       = schema + "value"

  test("fail to extract a missing key") {
    val missing       = schema + "xxx"
    val path          = List(DownField("https://schema.org/xxx"), DownArray, DownField("https://schema.org/drinks"), DownArray)
    val expectedError = KeyMissingFailure("@value", path)
    loadCursor.map {
      _.downField(drinks).downField(missing).get[String].assertLeft(expectedError)
    }
  }

  test("extract a string") {
    loadCursor.map {
      _.downField(drinks).downField(name).get[String].assertRight("Mojito")
    }
  }

  test("fail to extract a String") {
    val expectedError = ParsingFailure(
      s"Could not extract a 'String' from the path 'DownArray,DownField($drinks),DownArray,DownField($alcohol),DownArray,DownField(@value)'"
    )
    loadCursor.map { cursor =>
      val c = cursor.downField(drinks).downField(alcohol)
      c.get[String].assertLeft(expectedError)
      c.getOrElse("default").assertLeft(expectedError)
    }
  }

  test("extract default value") {
    loadCursor.map {
      _.downField(alcohol).getOrElse("default").assertRight("default")
    }
  }

  test("extract a Boolean") {
    loadCursor.map {
      _.downField(drinks).downField(alcohol).get[Boolean].assertRight(true)
    }
  }

  test("fail to extract a Boolean") {
    val expectedError = ParsingFailure(
      s"Could not convert 'Mojito' to 'boolean' from the path 'DownArray,DownField($drinks),DownArray,DownField($name),DownArray,DownField(@value)'"
    )
    loadCursor.map {
      _.downField(drinks).downField(name).get[Boolean].assertLeft(expectedError)
    }
  }

  test("extract a double") {
    loadCursor.map {
      _.downField(drinks).downField(volume).downField(value).get[Double].assertRight(8.3)
    }
  }

  test("fail to extract a double") {
    loadCursor.map {
      _.downField(drinks).downField(ingredients).get[Double].assertLeftOf[DecodingFailure]
    }
  }

  test("extract a Set of Strings") {
    loadCursor.map {
      _.downField(drinks).downField(ingredients).get[Set[String]].assertRight(Set("rum", "sugar"))
    }
  }

  test("extract a List of Strings") {
    loadCursor.map {
      _.downField(drinks).downField(steps).get[List[String]].assertRight(List("cut", "mix"))
    }
  }

  test("fail to extract a List of Strings") {
    val expectedError = ParsingFailure(
      s"Could not extract a 'Sequence' from the path 'DownArray,DownField($drinks),DownArray,DownField($ingredients),DownArray,DownField(@list)'"
    )
    loadCursor.map {
      _.downField(drinks).downField(ingredients).get[List[String]].assertLeft(expectedError)
    }
  }

  test("extract the types") {
    loadCursor.map { cursor =>
      cursor.getTypes.assertRight(Set(schema + "Menu", schema + "DrinkMenu"))
      cursor.downField(drinks).getTypes.assertRight(Set(schema + "Drink", schema + "Cocktail"))
    }
  }

  test("fail to extract the types") {
    val expectedError = ParsingFailure(s"Could not extract a 'Set[Iri]' from the path 'DownArray,DownField($alcohol)'")
    loadCursor.map {
      _.downField(alcohol).getTypes.assertLeft(expectedError)
    }
  }
}
