package ai.senscience.nexus.delta.sdk.jsonld

import ai.senscience.nexus.delta.rdf.IriOrBNode
import ai.senscience.nexus.delta.sdk.error.FormatErrors.{IllegalIdentityIriFormatError, IllegalSubjectIriFormatError}
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sourcing.model.Identity.*
import ai.senscience.nexus.delta.sourcing.model.{Identity, Label}
import ai.senscience.nexus.testkit.scalatest.BaseSpec

class IdentityIriEncodingSpec extends BaseSpec {

  implicit private val base: BaseUri                = BaseUri.unsafe("http://localhost:8080", "v1")
  private val realm                                 = Label.unsafe("myrealm")
  private val list: Seq[(IriOrBNode.Iri, Identity)] = List(
    iri"http://localhost:8080/v1/anonymous"                    -> Anonymous,
    iri"http://localhost:8080/v1/realms/$realm/users/myuser"   -> User("myuser", realm),
    iri"http://localhost:8080/v1/realms/$realm/groups/mygroup" -> Group("mygroup", realm),
    iri"http://localhost:8080/v1/realms/$realm/authenticated"  -> Authenticated(realm)
  )

  "An Identity" should {

    "be converted to an Iri" in {
      forAll(list) { case (iri, identity) =>
        identity.asIri shouldEqual iri
      }
    }

    "be created from an Iri" in {
      forAll(list) { case (iri, identity) =>
        iri.as[Identity].rightValue shouldEqual identity
      }
    }

    "failed to be created from an Iri" in {
      val failed = List(
        iri"http://localhost:8080/v1/other/anonymous",
        iri"http://localhost:8081/v1/anonymous",
        iri"http://localhost:8080/v1/realms/$realm/users/myuser/other"
      )
      forAll(failed) { iri =>
        iri.as[Identity].leftValue shouldBe a[IllegalIdentityIriFormatError]
      }
    }
  }

  "An Subject" should {

    "be converted to an Iri" in {
      forAll(list.take(2)) { case (iri, identity) =>
        identity.asIri shouldEqual iri
      }
    }

    "be created from an Iri" in {
      forAll(list.take(2)) { case (iri, identity) =>
        iri.as[Subject].rightValue shouldEqual identity.asInstanceOf[Subject]
      }
    }

    "failed to be created from an Iri" in {
      val failed = List(
        iri"http://localhost:8080/v1/other/anonymous",
        iri"http://localhost:8081/v1/anonymous",
        iri"http://localhost:8080/v1/realms/$realm/users/myuser/other"
      ) ++ list.slice(2, 4).map(_._1)
      forAll(failed) { iri =>
        iri.as[Subject].leftValue shouldBe a[IllegalSubjectIriFormatError]
      }
    }
  }
}
