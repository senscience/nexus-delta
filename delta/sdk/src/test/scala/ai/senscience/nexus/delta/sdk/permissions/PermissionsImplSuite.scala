package ai.senscience.nexus.delta.sdk.permissions

import ai.senscience.nexus.delta.sdk.ConfigFixtures
import ai.senscience.nexus.delta.sdk.generators.PermissionsGen
import ai.senscience.nexus.delta.sdk.generators.PermissionsGen.minimum
import ai.senscience.nexus.delta.sdk.permissions.model.Permission
import ai.senscience.nexus.delta.sdk.permissions.model.PermissionsRejection.*
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.{Identity, Label}
import ai.senscience.nexus.delta.sourcing.postgres.Doobie
import ai.senscience.nexus.testkit.mu.NexusSuite
import munit.AnyFixture

class PermissionsImplSuite extends NexusSuite with Doobie.Fixture with ConfigFixtures {

  override def munitFixtures: Seq[AnyFixture[?]] = List(doobie)

  private given subject: Subject = Identity.User("user", Label.unsafe("realm"))

  private val config = PermissionsConfig(
    eventLogConfig,
    PermissionsGen.minimum,
    Set.empty
  )

  private lazy val xas = doobie()

  private lazy val permissions: Permissions = PermissionsImpl(config, xas, clock)

  private val read: Permission = Permissions.permissions.read

  private val perm1: Permission = Permission.unsafe(genString())
  private val perm2: Permission = Permission.unsafe(genString())
  private val perm3: Permission = Permission.unsafe(genString())
  private val perm4: Permission = Permission.unsafe(genString())

  test("Echo the minimum permissions") {
    assertEquals(permissions.minimum, minimum)
  }

  test("Return the minimum permissions set") {
    permissions.fetchPermissionSet.assertEquals(minimum)
  }

  test("Return the minimum permissions resource") {
    permissions.fetch.assertEquals(PermissionsGen.resourceFor(minimum, rev = 0))
  }

  test("Fail to delete minimum when initial") {
    permissions.delete(0).interceptEquals(CannotDeleteMinimumCollection)
  }

  test("Fail to subtract with incorrect rev") {
    permissions.subtract(Set(perm1), 1).interceptEquals(IncorrectRev(1, 0))
  }

  test("Fail to subtract from minimum") {
    permissions.subtract(Set(perm1), 0).interceptEquals(CannotSubtractFromMinimumCollection(minimum))
  }

  test("Fail to subtract undefined permissions") {
    for {
      _ <- permissions.append(Set(perm1), 0)
      _ <- permissions.fetchPermissionSet.assertEquals(minimum + perm1)
      _ <- permissions.subtract(Set(perm2), 1).interceptEquals(CannotSubtractUndefinedPermissions(Set(perm2)))
    } yield ()
  }

  test("Fail to subtract empty permissions") {
    permissions.subtract(Set.empty, 1).interceptEquals(CannotSubtractEmptyCollection)
  }

  test("Fail to subtract from minimum collection") {
    permissions.subtract(Set(read), 1).interceptEquals(CannotSubtractFromMinimumCollection(minimum))
  }

  test("Subtract a permission") {
    for {
      _ <- permissions.subtract(Set(perm1), 1)
      _ <- permissions.fetchPermissionSet.assertEquals(minimum)
    } yield ()
  }

  test("Fail to append with incorrect rev") {
    permissions.append(Set(perm1), 0).interceptEquals(IncorrectRev(0, 2))
  }

  test("Append permissions") {
    for {
      _ <- permissions.append(Set(perm1, perm2), 2)
      _ <- permissions.fetchPermissionSet.assertEquals(minimum ++ Set(perm1, perm2))
    } yield ()
  }

  test("Fail to append duplicate permissions") {
    permissions.append(Set(perm2), 3).interceptEquals(CannotAppendEmptyCollection)
  }

  test("Fail to append empty permissions") {
    permissions.append(Set.empty, 3).interceptEquals(CannotAppendEmptyCollection)
  }

  test("Fail to replace with incorrect rev") {
    permissions.replace(Set(perm3), 1).interceptEquals(IncorrectRev(1, 3))
  }

  test("Fail to replace with empty permissions") {
    permissions.replace(Set.empty, 3).interceptEquals(CannotReplaceWithEmptyCollection)
  }

  test("Fail to replace with subset of minimum") {
    permissions.replace(Set(read), 3).interceptEquals(CannotReplaceWithEmptyCollection)
  }

  test("Replace non minimum") {
    for {
      _ <- permissions.replace(Set(perm3, perm4), 3)
      _ <- permissions.fetchPermissionSet.assertEquals(minimum ++ Set(perm3, perm4))
    } yield ()
  }

  test("Fail to delete with incorrect rev") {
    permissions.delete(2).interceptEquals(IncorrectRev(2, 4))
  }

  test("Delete permissions") {
    for {
      _ <- permissions.delete(4)
      _ <- permissions.fetchPermissionSet.assertEquals(minimum)
    } yield ()
  }

  test("Fail to delete minimum permissions") {
    permissions.delete(5).interceptEquals(CannotDeleteMinimumCollection)
  }

  test("Return minimum for revision 0") {
    permissions.fetchAt(0).map(_.value.permissions).assertEquals(minimum)
  }

  test("Return revision for correct rev") {
    permissions.fetchAt(4).map(_.value).assertEquals(model.PermissionSet(minimum ++ Set(perm3, perm4)))
  }

  test("Return none for unknown rev") {
    permissions.fetchAt(9999).interceptEquals(RevisionNotFound(9999, 5))
  }
}
