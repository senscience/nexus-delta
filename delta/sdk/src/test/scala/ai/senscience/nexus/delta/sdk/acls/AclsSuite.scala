package ai.senscience.nexus.delta.sdk.acls

import ai.senscience.nexus.delta.sdk.acls.Acls.{evaluate, next}
import ai.senscience.nexus.delta.sdk.acls.model.*
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress.Root
import ai.senscience.nexus.delta.sdk.acls.model.AclCommand.*
import ai.senscience.nexus.delta.sdk.acls.model.AclEvent.*
import ai.senscience.nexus.delta.sdk.acls.model.AclRejection.*
import ai.senscience.nexus.delta.sdk.permissions.model.Permission
import ai.senscience.nexus.delta.sourcing.model.Identity.{Anonymous, User}
import ai.senscience.nexus.delta.sourcing.model.Label
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.effect.IO

import java.time.Instant

class AclsSuite extends NexusSuite with AclFixtures {

  private val time2 = Instant.ofEpochMilli(10L)

  private val currentRealms       = Set(realm, realm2)
  private val fetchPermissionsSet = IO.pure(rwx)
  private val findUnknownRealms   = Acls.findUnknownRealms(_, currentRealms)
  private val currentState        = AclState(userR_groupX(Root), 1, epoch, Anonymous, epoch, Anonymous)

  private val eval: (Option[AclState], AclCommand) => IO[AclEvent] =
    evaluate(fetchPermissionsSet, findUnknownRealms, clock)

  // -----------------------------------------
  // Successful events
  // -----------------------------------------

  test("ReplaceAcl creates a new event from None") {
    val expectedEvent = AclReplaced(groupR(Root), 1, epoch, subject)
    eval(None, ReplaceAcl(groupR(Root), 0, subject)).assertEquals(expectedEvent)
  }

  test("AppendAcl creates a new event from None") {
    val expectedEvent = AclAppended(groupR(Root), 1, epoch, subject)
    eval(None, AppendAcl(groupR(Root), 0, subject)).assertEquals(expectedEvent)
  }

  test("ReplaceAcl creates a new event from existing state") {
    val expectedEvent = AclReplaced(userW(Root), 2, epoch, subject)
    eval(Some(currentState), ReplaceAcl(userW(Root), 1, subject)).assertEquals(expectedEvent)
  }

  test("AppendAcl creates a new event from existing state") {
    val expectedEvent = AclAppended(userW(Root), 2, epoch, subject)
    eval(Some(currentState), AppendAcl(userW(Root), 1, subject)).assertEquals(expectedEvent)
  }

  test("SubtractAcl creates a new event") {
    val expectedEvent = AclSubtracted(groupX(Root), 2, epoch, subject)
    eval(Some(currentState), SubtractAcl(groupX(Root), 1, subject)).assertEquals(expectedEvent)
  }

  test("DeleteAcl creates a new event") {
    val expectedEvent = AclDeleted(Root, 2, epoch, subject)
    eval(Some(currentState), DeleteAcl(Root, 1, subject)).assertEquals(expectedEvent)
  }

  // -----------------------------------------
  // IncorrectRev rejections
  // -----------------------------------------

  List(
    ("ReplaceAcl from None", None, ReplaceAcl(groupR(Root), 1, subject)),
    ("ReplaceAcl from existing", Some(currentState), ReplaceAcl(groupR(Root), 2, subject)),
    ("AppendAcl from None", None, AppendAcl(groupR(Root), 1, subject)),
    ("AppendAcl from existing", Some(currentState), AppendAcl(groupR(Root), 2, subject)),
    ("SubtractAcl", Some(currentState), SubtractAcl(groupR(Root), 2, subject)),
    ("DeleteAcl", Some(currentState), DeleteAcl(Root, 2, subject))
  ).foreach { case (name, state, cmd) =>
    test(s"$name is rejected with IncorrectRev") {
      eval(state, cmd).intercept[IncorrectRev]
    }
  }

  // -----------------------------------------
  // AclNotFound rejections
  // -----------------------------------------

  List(
    "SubtractAcl" -> SubtractAcl(groupR(Root), 0, subject),
    "DeleteAcl"   -> DeleteAcl(Root, 0, subject)
  ).foreach { case (name, cmd) =>
    test(s"$name is rejected with AclNotFound") {
      eval(None, cmd).intercept[AclNotFound]
    }
  }

  // -----------------------------------------
  // Other rejections
  // -----------------------------------------

  test("DeleteAcl is rejected with AclIsEmpty") {
    eval(Some(currentState.copy(acl = Acl(Root))), DeleteAcl(Root, 1, subject)).intercept[AclIsEmpty]
  }

  private val someEmptyPerms = groupR(Root) ++ Acl(Root, subject -> Set.empty[Permission])

  List(
    ("ReplaceAcl from None", None, ReplaceAcl(someEmptyPerms, 0, subject)),
    ("AppendAcl from None", None, AppendAcl(someEmptyPerms, 0, subject)),
    ("ReplaceAcl from existing", Some(currentState), ReplaceAcl(someEmptyPerms, 1, subject)),
    ("AppendAcl from existing", Some(currentState), AppendAcl(someEmptyPerms, 1, subject)),
    ("SubtractAcl", Some(currentState), SubtractAcl(someEmptyPerms, 1, subject))
  ).foreach { case (name, state, cmd) =>
    test(s"$name is rejected with AclCannotContainEmptyPermissionCollection") {
      eval(state, cmd).intercept[AclCannotContainEmptyPermissionCollection]
    }
  }

  List(
    "AppendAcl"   -> AppendAcl(userR_groupX(Root), 1, subject),
    "SubtractAcl" -> SubtractAcl(anonR(Root), 1, subject)
  ).foreach { case (name, cmd) =>
    test(s"$name is rejected with NothingToBeUpdated") {
      eval(Some(currentState), cmd).intercept[NothingToBeUpdated]
    }
  }

  private val unknownPermsAcl = Acl(Root, group -> Set(Permission.unsafe("other")))

  List(
    ("ReplaceAcl from None", None, ReplaceAcl(unknownPermsAcl, 0, subject)),
    ("AppendAcl from None", None, AppendAcl(unknownPermsAcl, 0, subject)),
    ("ReplaceAcl from existing", Some(currentState), ReplaceAcl(unknownPermsAcl, 1, subject)),
    ("AppendAcl from existing", Some(currentState), AppendAcl(unknownPermsAcl, 1, subject))
  ).foreach { case (name, state, cmd) =>
    test(s"$name is rejected with UnknownPermissions") {
      eval(state, cmd).intercept[UnknownPermissions]
    }
  }

  private val unknownRealm    = Label.unsafe("other-realm")
  private val unknownRealm2   = Label.unsafe("other-realm2")
  private val unknownRealmAcl =
    Acl(Root, User("myuser", unknownRealm) -> Set(r), User("myuser2", unknownRealm2) -> Set(r))

  List(
    ("ReplaceAcl from None", None, ReplaceAcl(unknownRealmAcl, 0, subject)),
    ("AppendAcl from None", None, AppendAcl(unknownRealmAcl, 0, subject)),
    ("ReplaceAcl from existing", Some(currentState), ReplaceAcl(unknownRealmAcl, 1, subject)),
    ("AppendAcl from existing", Some(currentState), AppendAcl(unknownRealmAcl, 1, subject))
  ).foreach { case (name, state, cmd) =>
    test(s"$name is rejected with UnknownRealms") {
      eval(state, cmd).interceptEquals(UnknownRealms(Set(unknownRealm, unknownRealm2)))
    }
  }

  // -----------------------------------------
  // State transitions (next function)
  // -----------------------------------------

  test("next: AclReplaced") {
    val event = AclReplaced(userW(Root), 1, time2, subject)
    assertEquals(
      next(None, event),
      Some(AclState(userW(Root), 1, time2, subject, time2, subject))
    )
    assertEquals(
      next(Some(currentState), event),
      Some(AclState(userW(Root), 1, epoch, Anonymous, time2, subject))
    )
  }

  test("next: AclAppended") {
    val event = AclAppended(userW(Root), 1, time2, subject)
    assertEquals(
      next(None, event),
      Some(AclState(userW(Root), 1, time2, subject, time2, subject))
    )
    assertEquals(
      next(Some(currentState), event),
      Some(AclState(userRW_groupX(Root), 1, epoch, Anonymous, time2, subject))
    )
  }

  test("next: AclSubtracted") {
    assertEquals(next(None, AclSubtracted(groupX(Root), 1, epoch, subject)), None)
    assertEquals(
      next(Some(currentState), AclSubtracted(groupX(Root), 1, time2, subject)),
      Some(AclState(userR(Root), 1, epoch, Anonymous, time2, subject))
    )
  }

  test("next: AclDeleted") {
    assertEquals(next(None, AclDeleted(Root, 1, epoch, subject)), None)
    assertEquals(
      next(Some(currentState), AclDeleted(Root, 1, time2, subject)),
      Some(AclState(Acl(Root), 1, epoch, Anonymous, time2, subject))
    )
  }
}
