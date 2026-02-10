package ai.senscience.nexus.delta.sdk.model

import ai.senscience.nexus.delta.rdf.Vocabulary.{nxv, schema, schemas}
import ai.senscience.nexus.delta.sdk.model.IdSegment.{IriSegment, StringSegment}
import ai.senscience.nexus.delta.sdk.projects.model.{ApiMappings, ProjectBase}
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.testkit.mu.NexusSuite

class IdSegmentSuite extends NexusSuite {

  private val am   = ApiMappings(
    "nxv"      -> nxv.base,
    "data"     -> schemas.base,
    "Person"   -> schema.Person,
    "_"        -> schemas.resources,
    "resource" -> schemas.resources
  )
  private val base = ProjectBase(nxv.base)

  test("A string segment should be converted to an Iri") {
    val list =
      List(
        "data:other"         -> (schemas + "other"),
        "nxv:other"          -> (nxv + "other"),
        "Person"             -> schema.Person,
        "_"                  -> schemas.resources,
        "other"              -> (nxv + "other"),
        "http://example.com" -> iri"http://example.com"
      )
    list.foreach { case (string, iri) =>
      assertEquals(StringSegment(string).toIri(am, base), Some(iri))
    }
  }

  test("A string segment should fail to be converted to an Iri") {
    assertEquals(StringSegment("#a?!*#").toIri(am, base), None)
  }

  test("An Iri segment should be converted to an Iri") {
    val list =
      List(
        nxv + "other"           -> (nxv + "other"),
        schema.Person           -> schema.Person,
        schemas.resources       -> schemas.resources,
        iri"http://example.com" -> iri"http://example.com",
        iri"data:other"         -> (schemas + "other")
      )
    list.foreach { case (iri, expected) =>
      assertEquals(IriSegment(iri).toIri(am, base), Some(expected))
    }
  }
}
