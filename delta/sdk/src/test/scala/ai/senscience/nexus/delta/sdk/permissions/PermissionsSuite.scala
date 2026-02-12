package ai.senscience.nexus.delta.sdk.permissions

import ai.senscience.nexus.delta.sdk.permissions.Permissions.{acls, permissions}
import ai.senscience.nexus.delta.sdk.permissions.model.*
import ai.senscience.nexus.delta.sdk.permissions.model.PermissionsCommand.*
import ai.senscience.nexus.delta.sdk.permissions.model.PermissionsEvent.*
import ai.senscience.nexus.delta.sdk.permissions.model.PermissionsRejection.*
import ai.senscience.nexus.delta.sourcing.model.{Identity, Label}
import ai.senscience.nexus.testkit.mu.NexusSuite

import java.time.Instant

class PermissionsSuite extends NexusSuite {

  private val minimum     = Set(permissions.write, permissions.read)
  private val appended    = Set(acls.write, acls.read)
  private val subtracted  = Set(acls.write)
  private val unknown     = Set(Permission.unsafe("unknown/unknown"))
  private val epoch       = Instant.EPOCH
  private val instantNext = epoch.plusMillis(1L)
  private val subject     = Identity.User("user", Label.unsafe("realm"))
  private val subjectNext = Identity.User("next-user", Label.unsafe("realm"))

  private val initial = PermissionsState.initial(minimum)
  private val next    = Permissions.next(minimum)(_, _)
  private val eval    = Permissions.evaluate(minimum, clock)(_, _)

  // -----------------------------------------
  // Successful events
  // -----------------------------------------

  test("Replace permissions when state is initial") {
    val cmd = ReplacePermissions(0, appended, subjectNext)
    eval(initial, cmd).assertEquals(PermissionsReplaced(1, appended, epoch, subjectNext))
  }

  test("Replace permissions when state is current") {
    val state = PermissionsState(1, minimum, epoch, subject, epoch, subject)
    val cmd   = ReplacePermissions(1, appended, subjectNext)
    eval(state, cmd).assertEquals(PermissionsReplaced(2, appended, epoch, subjectNext))
  }

  test("Append permissions when state is initial") {
    val cmd = AppendPermissions(0, appended, subjectNext)
    eval(initial, cmd).assertEquals(PermissionsAppended(1, appended, epoch, subjectNext))
  }

  test("Append permissions when state is current") {
    val state = PermissionsState(1, minimum, epoch, subject, epoch, subject)
    val cmd   = AppendPermissions(1, minimum ++ appended, subjectNext)
    eval(state, cmd).assertEquals(PermissionsAppended(2, appended, epoch, subjectNext))
  }

  test("Subtract permissions when state is current") {
    val state = PermissionsState(1, minimum ++ appended, epoch, subject, epoch, subject)
    val cmd   = SubtractPermissions(1, minimum ++ subtracted, subjectNext)
    eval(state, cmd).assertEquals(PermissionsSubtracted(2, subtracted, epoch, subjectNext))
  }

  test("Delete permissions when state is current") {
    val state = PermissionsState(1, minimum ++ appended, epoch, subject, epoch, subject)
    val cmd   = DeletePermissions(1, subjectNext)
    eval(state, cmd).assertEquals(PermissionsDeleted(2, epoch, subjectNext))
  }

  // -----------------------------------------
  // IncorrectRev rejections
  // -----------------------------------------

  List(
    ("initial, ReplacePermissions", initial, ReplacePermissions(1, appended, subjectNext), IncorrectRev(1, 0)),
    ("initial, AppendPermissions", initial, AppendPermissions(1, appended, subjectNext), IncorrectRev(1, 0)),
    ("initial, SubtractPermissions", initial, SubtractPermissions(1, subtracted, subjectNext), IncorrectRev(1, 0)),
    ("initial, DeletePermissions", initial, DeletePermissions(1, subjectNext), IncorrectRev(1, 0)),
    (
      "current, ReplacePermissions",
      PermissionsState(1, minimum, epoch, subject, epoch, subject),
      ReplacePermissions(2, appended, subjectNext),
      IncorrectRev(2, 1)
    ),
    (
      "current, AppendPermissions",
      PermissionsState(1, minimum, epoch, subject, epoch, subject),
      AppendPermissions(2, appended, subjectNext),
      IncorrectRev(2, 1)
    ),
    (
      "current, SubtractPermissions",
      PermissionsState(1, minimum, epoch, subject, epoch, subject),
      SubtractPermissions(2, subtracted, subjectNext),
      IncorrectRev(2, 1)
    ),
    (
      "current, DeletePermissions",
      PermissionsState(1, minimum, epoch, subject, epoch, subject),
      DeletePermissions(2, subjectNext),
      IncorrectRev(2, 1)
    )
  ).foreach { case (name, state, cmd, expected) =>
    test(s"Reject with IncorrectRev for $name") {
      eval(state, cmd).interceptEquals(expected)
    }
  }

  // -----------------------------------------
  // CannotReplaceWithEmptyCollection rejections
  // -----------------------------------------

  test("Reject with CannotReplaceWithEmptyCollection when permission set is empty") {
    val state = PermissionsState(1, minimum, epoch, subject, epoch, subject)
    val cmd   = ReplacePermissions(1, Set.empty, subjectNext)
    eval(state, cmd).interceptEquals(CannotReplaceWithEmptyCollection)
  }

  test("Reject with CannotReplaceWithEmptyCollection when permission set is minimum") {
    val state = PermissionsState(1, minimum, epoch, subject, epoch, subject)
    val cmd   = ReplacePermissions(1, minimum, subjectNext)
    eval(state, cmd).interceptEquals(CannotReplaceWithEmptyCollection)
  }

  // -----------------------------------------
  // CannotAppendEmptyCollection rejections
  // -----------------------------------------

  test("Reject with CannotAppendEmptyCollection when permission set is empty") {
    val state = PermissionsState(1, minimum, epoch, subject, epoch, subject)
    val cmd   = AppendPermissions(1, Set.empty, subjectNext)
    eval(state, cmd).interceptEquals(CannotAppendEmptyCollection)
  }

  test("Reject with CannotAppendEmptyCollection when permission set is minimum while state is initial") {
    eval(initial, AppendPermissions(0, minimum, subjectNext)).interceptEquals(CannotAppendEmptyCollection)
  }

  test("Reject with CannotAppendEmptyCollection when permission set is minimum while state is current") {
    val state = PermissionsState(1, minimum, epoch, subject, epoch, subject)
    val cmd   = AppendPermissions(1, minimum, subjectNext)
    eval(state, cmd).interceptEquals(CannotAppendEmptyCollection)
  }

  test("Reject with CannotAppendEmptyCollection when permission set is a subset of current permissions") {
    val state = PermissionsState(1, minimum ++ appended, epoch, subject, epoch, subject)
    val cmd   = AppendPermissions(1, appended, subjectNext)
    eval(state, cmd).interceptEquals(CannotAppendEmptyCollection)
  }

  // -----------------------------------------
  // CannotSubtractEmptyCollection rejections
  // -----------------------------------------

  test("Reject with CannotSubtractEmptyCollection when permission set is empty") {
    val state = PermissionsState(1, minimum, epoch, subject, epoch, subject)
    val cmd   = SubtractPermissions(1, Set.empty, subjectNext)
    eval(state, cmd).interceptEquals(CannotSubtractEmptyCollection)
  }

  // -----------------------------------------
  // CannotSubtractFromMinimumCollection rejections
  // -----------------------------------------

  test("Reject with CannotSubtractFromMinimumCollection when permission set is minimum and state is initial") {
    eval(initial, SubtractPermissions(0, minimum, subjectNext))
      .interceptEquals(CannotSubtractFromMinimumCollection(minimum))
  }

  test("Reject with CannotSubtractFromMinimumCollection when state has more permissions") {
    val state = PermissionsState(1, minimum ++ appended, epoch, subject, epoch, subject)
    val cmd   = SubtractPermissions(1, minimum, subjectNext)
    eval(state, cmd).interceptEquals(CannotSubtractFromMinimumCollection(minimum))
  }

  test("Reject with CannotSubtractFromMinimumCollection when permission set is minimum") {
    val state = PermissionsState(1, minimum, epoch, subject, epoch, subject)
    val cmd   = SubtractPermissions(1, minimum, subjectNext)
    eval(state, cmd).interceptEquals(CannotSubtractFromMinimumCollection(minimum))
  }

  // -----------------------------------------
  // CannotSubtractUndefinedPermissions rejections
  // -----------------------------------------

  test("Reject with CannotSubtractUndefinedPermissions when permissions are not in set") {
    val state = PermissionsState(1, minimum ++ appended, epoch, subject, epoch, subject)
    val cmd   = SubtractPermissions(1, minimum ++ subtracted ++ unknown, subjectNext)
    eval(state, cmd).interceptEquals(CannotSubtractUndefinedPermissions(unknown))
  }

  // -----------------------------------------
  // CannotDeleteMinimumCollection rejections
  // -----------------------------------------

  test("Reject with CannotDeleteMinimumCollection when state is initial") {
    eval(initial, DeletePermissions(0, subjectNext)).interceptEquals(CannotDeleteMinimumCollection)
  }

  test("Reject with CannotDeleteMinimumCollection when current permission set is the minimum") {
    val state = PermissionsState(1, minimum, epoch, subject, epoch, subject)
    val cmd   = DeletePermissions(1, subjectNext)
    eval(state, cmd).interceptEquals(CannotDeleteMinimumCollection)
  }

  // -----------------------------------------
  // State transitions (next function)
  // -----------------------------------------

  test("State is Initial and event is PermissionsAppended") {
    val event    = PermissionsAppended(1, appended, epoch, subject)
    val expected = PermissionsState(1, minimum ++ appended, epoch, subject, epoch, subject)
    assertEquals(next(initial, event), expected)
  }

  test("State is Initial and event is PermissionsSubtracted") {
    val event    = PermissionsSubtracted(1, subtracted, epoch, subject)
    val expected = PermissionsState(1, minimum, epoch, subject, epoch, subject)
    assertEquals(next(initial, event), expected)
  }

  test("State is Initial and event is PermissionsDeleted") {
    val event    = PermissionsDeleted(1, epoch, subject)
    val expected = PermissionsState(1, minimum, epoch, subject, epoch, subject)
    assertEquals(next(initial, event), expected)
  }

  test("State is Initial and event is PermissionsReplaced") {
    val event    = PermissionsReplaced(1, appended, epoch, subject)
    val expected = PermissionsState(1, minimum ++ appended, epoch, subject, epoch, subject)
    assertEquals(next(initial, event), expected)
  }

  test("State is Current and event is PermissionsAppended") {
    val state    = PermissionsState(1, minimum, epoch, subject, epoch, subject)
    val event    = PermissionsAppended(2, appended, instantNext, subjectNext)
    val expected = PermissionsState(2, minimum ++ appended, epoch, subject, instantNext, subjectNext)
    assertEquals(next(state, event), expected)
  }

  test("State is Current and event is PermissionsSubtracted") {
    val state    = PermissionsState(1, minimum ++ appended, epoch, subject, epoch, subject)
    val event    = PermissionsSubtracted(2, subtracted, instantNext, subjectNext)
    val expected = PermissionsState(2, appended -- subtracted ++ minimum, epoch, subject, instantNext, subjectNext)
    assertEquals(next(state, event), expected)
  }

  test("State is Current and event is PermissionsDeleted") {
    val state    = PermissionsState(1, minimum ++ appended, epoch, subject, epoch, subject)
    val event    = PermissionsDeleted(2, instantNext, subjectNext)
    val expected = PermissionsState(2, minimum, epoch, subject, instantNext, subjectNext)
    assertEquals(next(state, event), expected)
  }

  test("State is Current and event is PermissionsReplaced") {
    val state    = PermissionsState(1, minimum ++ appended, epoch, subject, epoch, subject)
    val event    = PermissionsReplaced(2, subtracted, instantNext, subjectNext)
    val expected = PermissionsState(2, minimum ++ subtracted, epoch, subject, instantNext, subjectNext)
    assertEquals(next(state, event), expected)
  }
}
