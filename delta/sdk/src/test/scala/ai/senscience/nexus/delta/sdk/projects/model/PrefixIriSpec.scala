package ai.senscience.nexus.delta.sdk.projects.model

import ai.senscience.nexus.delta.rdf.Vocabulary.*
import ai.senscience.nexus.delta.sdk.error.FormatErrors.{IllegalIRIFormatError, IllegalPrefixIRIFormatError}
import ai.senscience.nexus.testkit.scalatest.BaseSpec
import io.circe.Json
import io.circe.parser.*
import io.circe.syntax.*

class PrefixIriSpec extends BaseSpec {
  "A PrefixIri" should {

    "be an iri ending with / or #" in {
      forAll(List(xsd.base, schema.base, owl.base, rdf.base)) { iri =>
        PrefixIri.apply(iri).rightValue.value shouldEqual iri
        PrefixIri(iri.toString).rightValue.value shouldEqual iri
      }
    }

    "reject to be constructed" in {
      forAll(List(xsd.int, schema.Person, rdf.tpe)) { iri =>
        PrefixIri(iri).leftValue shouldBe a[IllegalPrefixIRIFormatError]
      }
      PrefixIri("f7*#?n\\?#3").leftValue shouldBe a[IllegalIRIFormatError]
    }

    "be encoded correctly to Json" in {
      forAll(List(xsd.base, schema.base, owl.base, rdf.base)) { iri =>
        PrefixIri(iri).rightValue.asJson shouldEqual Json.fromString(iri.toString)
      }
    }

    "be decoded correctly from Json" in {
      forAll(List(xsd.base, schema.base, owl.base, rdf.base)) { iri =>
        val str = s""""${iri.toString}""""
        decode[PrefixIri](str).rightValue
      }
    }

    "fail decoding from incorrect Json string format" in {
      forAll(List(xsd.int, schema.Person, rdf.tpe)) { iri =>
        val str = s""""${iri.toString}""""
        decode[PrefixIri](str).leftValue
      }
    }
  }
}
