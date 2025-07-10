package ai.senscience.nexus.delta.sdk.projects.model

import ai.senscience.nexus.delta.rdf.Vocabulary.{owl, rdf, schema, xsd}
import ai.senscience.nexus.delta.sdk.error.FormatErrors.{IllegalIRIFormatError, IllegalPrefixIRIFormatError}
import ai.senscience.nexus.testkit.mu.NexusSuite
import io.circe.Json
import io.circe.parser.decode
import io.circe.syntax.EncoderOps

class PrefixIriSuite extends NexusSuite {

  List(xsd.base, schema.base, owl.base, rdf.base).foreach { valid =>
    test(s"A Prefix iri can be constructed with '$valid'") {
      val result = PrefixIri(valid)
      result.map(_.value).assertRight(valid)
    }

    test(s"'$valid' can be encoded correctly to Json") {
      val result = PrefixIri.unsafe(valid)
      assertEquals(result.asJson, Json.fromString(valid.toString))
    }

    test(s"'$valid' decoded correctly from Json") {
      val expected = PrefixIri.unsafe(valid)
      val str      = s""""${valid.toString}""""
      decode[PrefixIri](str).assertRight(expected)
    }
  }

  test("Reject construction from string") {
    PrefixIri("f7*#?n\\?#3").assertLeftOf[IllegalIRIFormatError]
  }

  List(xsd.int, schema.Person, rdf.tpe).foreach { invalid =>
    test(s"'$invalid' reject to be constructed") {
      PrefixIri(invalid).assertLeftOf[IllegalPrefixIRIFormatError]
    }

    test(s"'$invalid' fails decoding as a prefix iri") {
      decode[PrefixIri](invalid.toString).assertLeft()
    }
  }
}
