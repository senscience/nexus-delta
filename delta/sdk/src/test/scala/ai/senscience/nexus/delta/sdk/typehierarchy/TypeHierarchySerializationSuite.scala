package ai.senscience.nexus.delta.sdk.typehierarchy

import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.rdf.syntax.iriStringContextSyntax
import ai.senscience.nexus.delta.sdk.SerializationSuite
import ai.senscience.nexus.delta.sdk.typehierarchy.model.TypeHierarchy.TypeHierarchyMapping
import ai.senscience.nexus.delta.sdk.typehierarchy.model.TypeHierarchyEvent.{TypeHierarchyCreated, TypeHierarchyUpdated}
import ai.senscience.nexus.delta.sdk.typehierarchy.model.{TypeHierarchyEvent, TypeHierarchyState}
import ai.senscience.nexus.delta.sourcing.model.Identity.{Subject, User}
import ai.senscience.nexus.delta.sourcing.model.Label

import java.time.Instant

class TypeHierarchySerializationSuite extends SerializationSuite {

  val realm: Label                  = Label.unsafe("myrealm")
  val subject: Subject              = User("username", realm)
  val instant: Instant              = Instant.EPOCH
  val id                            = nxv + "TypeHierarchy"
  val mapping: TypeHierarchyMapping = Map(
    iri"https://schema.org/Movie" -> Set(iri"https://schema.org/CreativeWork", iri"https://schema.org/Thing")
  )

  private val typeHierarchyEventMapping = Map(
    // format: off
    TypeHierarchyCreated(mapping, 1, instant, subject) -> loadDatabaseEvents("type-hierarchy", "type-hierarchy-created.json"),
    TypeHierarchyUpdated(mapping, 2, instant, subject) -> loadDatabaseEvents("type-hierarchy", "type-hierarchy-updated.json"),
    // format: on
  )

  typeHierarchyEventMapping.foreach { case (event, database) =>
    test(s"Correctly serialize ${event.getClass.getName}") {
      assertOutput(TypeHierarchyEvent.serializer, event, database)
    }

    test(s"Correctly deserialize ${event.getClass.getName}") {
      assertEquals(TypeHierarchyEvent.serializer.codec.decodeJson(database), Right(event))
    }
  }

  private val state = TypeHierarchyState(
    mapping,
    rev = 1,
    deprecated = false,
    createdAt = instant,
    createdBy = subject,
    updatedAt = instant,
    updatedBy = subject
  )

  private val jsonState = jsonContentOf("type-hierarchy/type-hierarchy-state.json")

  test(s"Correctly serialize an TypeHierarchyState") {
    assertOutput(TypeHierarchyState.serializer, state, jsonState)
  }

  test(s"Correctly deserialize an TypeHierarchyState") {
    assertEquals(TypeHierarchyState.serializer.codec.decodeJson(jsonState), Right(state))
  }

}
