package ai.senscience.nexus.delta.sdk.jsonld

import ai.senscience.nexus.delta.rdf.IriOrBNode
import ai.senscience.nexus.delta.sdk.error.FormatErrors.{IllegalIdentityIriFormatError, IllegalSubjectIriFormatError}
import ai.senscience.nexus.delta.sdk.implicits.{given, *}
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sourcing.model.Identity.*
import ai.senscience.nexus.delta.sourcing.model.{Identity, Label}
import ai.senscience.nexus.testkit.mu.NexusSuite

class IdentityIriEncodingSuite extends NexusSuite {

  private given BaseUri  = BaseUri.unsafe("http://localhost:8080", "v1")
  private val realm      = Label.unsafe("myrealm")
  private val subjects   = List(
    iri"http://localhost:8080/v1/anonymous"                  -> Anonymous,
    iri"http://localhost:8080/v1/realms/$realm/users/myuser" -> User("myuser", realm)
  )
  private val group      = iri"http://localhost:8080/v1/realms/$realm/groups/mygroup" -> Group("mygroup", realm)
  private val auth       = iri"http://localhost:8080/v1/realms/$realm/authenticated"  -> Authenticated(realm)
  private val identities = subjects ++ List(group, auth)

  test("An Identity should be converted to an Iri") {
    identities.foreach { case (iri, identity) =>
      assertEquals(identity.asIri, iri, s"Identity '$identity' was not converted to the expected Iri.")
    }
  }

  test("An Identity should be created from an Iri") {
    identities.foreach { case (iri, identity) =>
      iri.as[Identity].assertRight(identity, s"Iri '$iri' was not converted to the expected Identity.")
    }
  }

  test("An Identity should fail to be created from an invalid Iri") {
    val failed = List(
      iri"http://localhost:8080/v1/other/anonymous",
      iri"http://localhost:8081/v1/anonymous",
      iri"http://localhost:8080/v1/realms/$realm/users/myuser/other"
    )
    failed.foreach { iri =>
      iri.as[Identity].assertLeftOf[IllegalIdentityIriFormatError](s"Iri '$iri' should have failed.")
    }
  }

  test("A Subject should be converted to an Iri") {
    identities.take(2).foreach { case (iri, identity) =>
      assertEquals(identity.asIri, iri, s"Subject '$identity' was not converted to the expected Iri.")
    }
  }

  test("A Subject should be created from an Iri") {
    subjects.foreach { case (iri, identity) =>
      iri.as[Subject].assertRight(identity, s"Iri '$iri' was not converted to the expected Subject.")
    }
  }

  test("A Subject should fail to be created from an invalid Iri") {
    val failed = List(
      iri"http://localhost:8080/v1/other/anonymous",
      iri"http://localhost:8081/v1/anonymous",
      iri"http://localhost:8080/v1/realms/$realm/users/myuser/other"
    ) ++ List(group, auth).map(_._1)
    failed.foreach { iri =>
      iri.as[Subject].assertLeftOf[IllegalSubjectIriFormatError](s"Iri '$iri' should have failed.")
    }
  }
}
