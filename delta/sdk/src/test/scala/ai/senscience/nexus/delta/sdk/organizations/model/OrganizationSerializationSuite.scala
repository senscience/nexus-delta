package ai.senscience.nexus.delta.sdk.organizations.model

import ai.senscience.nexus.delta.sdk.SerializationSuite
import ai.senscience.nexus.delta.sdk.organizations.model.OrganizationEvent.{OrganizationCreated, OrganizationDeprecated, OrganizationUndeprecated, OrganizationUpdated}
import ai.senscience.nexus.delta.sourcing.model.Identity.{Subject, User}
import ai.senscience.nexus.delta.sourcing.model.Label

import java.time.Instant
import java.util.UUID

class OrganizationSerializationSuite extends SerializationSuite {

  val realm: Label        = Label.unsafe("myrealm")
  val subject: Subject    = User("username", realm)
  val org: Label          = Label.unsafe("myorg")
  val orgUuid: UUID       = UUID.fromString("b6bde92f-7836-4da6-8ead-2e0fd516ebe7")
  val description: String = "some description"
  val instant: Instant    = Instant.EPOCH
  val rev                 = 1

  private val orgsEventMapping = Map(
    // format: off
    OrganizationCreated(org, orgUuid, 1, Some(description), instant, subject) -> loadDatabaseEvents("organizations", "org-created.json"),
    OrganizationUpdated(org, orgUuid, 1, Some(description), instant, subject) -> loadDatabaseEvents("organizations", "org-updated.json"),
    OrganizationDeprecated(org, orgUuid, 1, instant, subject)                 -> loadDatabaseEvents("organizations", "org-deprecated.json"),
    OrganizationUndeprecated(org, orgUuid, 1, instant, subject)               -> loadDatabaseEvents("organizations", "org-undeprecated.json")
    // format: on
  )

  orgsEventMapping.foreach { case (event, database) =>
    test(s"Correctly serialize ${event.getClass.getName}") {
      assertOutput(OrganizationEvent.serializer, event, database)
    }

    test(s"Correctly deserialize ${event.getClass.getName}") {
      assertEquals(OrganizationEvent.serializer.codec.decodeJson(database), Right(event))
    }
  }

  private val state = OrganizationState(
    org,
    orgUuid,
    rev = rev,
    deprecated = false,
    description = Some(description),
    createdAt = instant,
    createdBy = subject,
    updatedAt = instant,
    updatedBy = subject
  )

  private val jsonState = jsonContentOf("organizations/org-state.json")

  test(s"Correctly serialize an OrganizationState") {
    assertOutput(OrganizationState.serializer, state, jsonState)
  }

  test(s"Correctly deserialize an OrganizationState") {
    assertEquals(OrganizationState.serializer.codec.decodeJson(jsonState), Right(state))
  }

}
