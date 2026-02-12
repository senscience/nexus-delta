package ai.senscience.nexus.delta.sdk.organizations

import ai.senscience.nexus.delta.kernel.utils.UUIDF
import ai.senscience.nexus.delta.sdk.generators.OrganizationGen
import ai.senscience.nexus.delta.sdk.organizations.Organizations.{evaluate, next}
import ai.senscience.nexus.delta.sdk.organizations.model.OrganizationCommand.*
import ai.senscience.nexus.delta.sdk.organizations.model.OrganizationEvent.*
import ai.senscience.nexus.delta.sdk.organizations.model.OrganizationRejection.*
import ai.senscience.nexus.delta.sdk.organizations.model.OrganizationState
import ai.senscience.nexus.delta.sourcing.model.Identity.User
import ai.senscience.nexus.testkit.mu.NexusSuite

import java.time.Instant

class OrganizationsSuite extends NexusSuite {

  private val epoch: Instant             = Instant.EPOCH
  private val time2: Instant             = Instant.ofEpochMilli(10L)
  private val state: OrganizationState   = OrganizationGen.state("org", 1, description = Some("desc"))
  private val deprecatedState            = state.copy(deprecated = true)
  private val (label, uuid, desc, desc2) = (state.label, state.uuid, state.description, Some("other"))
  private val subject: User              = User("myuser", label)

  private given UUIDF = UUIDF.fixed(uuid)

  // -----------------------------------------
  // Successful events
  // -----------------------------------------

  test("CreateOrganization creates a new event") {
    val expected = OrganizationCreated(label, uuid, 1, desc, epoch, subject)
    evaluate(clock)(None, CreateOrganization(label, desc, subject)).assertEquals(expected)
  }

  test("UpdateOrganization creates a new event") {
    val expected = OrganizationUpdated(label, uuid, 2, desc2, epoch, subject)
    evaluate(clock)(Some(state), UpdateOrganization(label, 1, desc2, subject)).assertEquals(expected)
  }

  test("DeprecateOrganization creates a new event") {
    val expected = OrganizationDeprecated(label, uuid, 2, epoch, subject)
    evaluate(clock)(Some(state), DeprecateOrganization(label, 1, subject)).assertEquals(expected)
  }

  test("UndeprecateOrganization creates a new event") {
    val expected = OrganizationUndeprecated(label, uuid, 2, epoch, subject)
    evaluate(clock)(Some(deprecatedState), UndeprecateOrganization(label, 1, subject)).assertEquals(expected)
  }

  // -----------------------------------------
  // IncorrectRev rejections
  // -----------------------------------------

  List(
    ("UpdateOrganization", state, UpdateOrganization(label, 2, desc2, subject)),
    ("DeprecateOrganization", state, DeprecateOrganization(label, 2, subject)),
    ("UndeprecateOrganization", deprecatedState, UndeprecateOrganization(label, 2, subject))
  ).foreach { case (name, state, cmd) =>
    test(s"$name is rejected with IncorrectRev") {
      evaluate(clock)(Some(state), cmd).intercept[IncorrectRev]
    }
  }

  // -----------------------------------------
  // OrganizationAlreadyExists rejection
  // -----------------------------------------

  test("CreateOrganization is rejected with OrganizationAlreadyExists") {
    evaluate(clock)(Some(state), CreateOrganization(label, desc, subject)).intercept[OrganizationAlreadyExists]
  }

  // -----------------------------------------
  // OrganizationIsDeprecated rejections
  // -----------------------------------------

  List(
    ("UpdateOrganization", UpdateOrganization(label, 1, desc2, subject)),
    ("DeprecateOrganization", DeprecateOrganization(label, 1, subject))
  ).foreach { case (name, cmd) =>
    test(s"$name is rejected with OrganizationIsDeprecated") {
      evaluate(clock)(Some(state.copy(deprecated = true)), cmd).intercept[OrganizationIsDeprecated]
    }
  }

  // -----------------------------------------
  // OrganizationIsNotDeprecated rejection
  // -----------------------------------------

  test("UndeprecateOrganization is rejected with OrganizationIsNotDeprecated") {
    evaluate(clock)(Some(state), UndeprecateOrganization(label, 1, subject)).intercept[OrganizationIsNotDeprecated]
  }

  // -----------------------------------------
  // OrganizationNotFound rejections
  // -----------------------------------------

  List(
    ("UpdateOrganization", UpdateOrganization(label, 1, desc2, subject)),
    ("DeprecateOrganization", DeprecateOrganization(label, 1, subject)),
    ("UndeprecateOrganization", UndeprecateOrganization(label, 1, subject))
  ).foreach { case (name, cmd) =>
    test(s"$name is rejected with OrganizationNotFound") {
      evaluate(clock)(None, cmd).intercept[OrganizationNotFound]
    }
  }

  // -----------------------------------------
  // State transitions (next function)
  // -----------------------------------------

  test("next: OrganizationCreated") {
    val event    = OrganizationCreated(label, uuid, 1, desc, time2, subject)
    val expected = state.copy(createdAt = time2, createdBy = subject, updatedAt = time2, updatedBy = subject)
    assertEquals(next(None, event), Some(expected))
    assertEquals(next(Some(state), event), None)
  }

  test("next: OrganizationUpdated") {
    val event    = OrganizationUpdated(label, uuid, 2, desc2, time2, subject)
    val expected = state.copy(rev = 2, description = desc2, updatedAt = time2, updatedBy = subject)
    assertEquals(next(None, event), None)
    assertEquals(next(Some(state), event), Some(expected))
  }

  test("next: OrganizationDeprecated") {
    val event    = OrganizationDeprecated(label, uuid, 2, time2, subject)
    val expected = state.copy(rev = 2, deprecated = true, updatedAt = time2, updatedBy = subject)
    assertEquals(next(None, event), None)
    assertEquals(next(Some(state), event), Some(expected))
  }

  test("next: OrganizationUndeprecated") {
    val event    = OrganizationUndeprecated(label, uuid, 2, time2, subject)
    val expected = deprecatedState.copy(rev = 2, deprecated = false, updatedAt = time2, updatedBy = subject)
    assertEquals(next(None, event), None)
    assertEquals(next(Some(deprecatedState), event), Some(expected))
  }

}
