package ai.senscience.nexus.delta.sdk.acls

import ai.senscience.nexus.delta.sdk.acls.AclCheckSuite.{ProjectValue, Value}
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress.Root
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.permissions.Permissions.*
import ai.senscience.nexus.delta.sdk.permissions.model.Permission
import ai.senscience.nexus.delta.sourcing.model.Identity.{Anonymous, Authenticated, Group, User}
import ai.senscience.nexus.delta.sourcing.model.{Label, ProjectRef}
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.effect.IO

class AclCheckSuite extends NexusSuite {

  private val realm         = Label.unsafe("wonderland")
  private val authenticated = Authenticated(realm)
  private val aliceUser     = User("alice", realm)
  private val alice         = Caller(aliceUser, Set(aliceUser, Anonymous, authenticated, Group("group", realm)))
  private val bobUser       = User("bob", realm)
  private val bob           = Caller(bobUser, Set(bobUser))
  private val anonymous     = Caller(Anonymous, Set(Anonymous))

  private val org1   = Label.unsafe("org1")
  private val proj11 = ProjectRef.unsafe("org1", "proj1")
  private val proj12 = ProjectRef.unsafe("org1", "proj2")

  private val org1Address            = AclAddress.Organization(org1)
  private val proj1Address           = AclAddress.Project(proj11)
  private val read: Permission       = resources.read
  private val write: Permission      = resources.write
  private val eventsRead: Permission = events.read
  private val aclCheck               = AclSimpleCheck(
    (Anonymous, Root, Set(eventsRead)),
    (aliceUser, org1Address, Set(read, write)),
    (bobUser, proj1Address, Set(read))
  ).accepted

  private val unauthorized = new IllegalArgumentException("The user has no access to this resource.")

  test("Return the acls provided at initialization") {
    aclCheck.authorizeFor(Root, eventsRead)(using anonymous).assertEquals(true) >>
      aclCheck.authorizeFor(org1Address, read)(using alice).assertEquals(true) >>
      aclCheck.authorizeFor(org1Address, write)(using alice).assertEquals(true) >>
      aclCheck.authorizeFor(proj1Address, read)(using bob).assertEquals(true)
  }

  List(alice, bob).foreach { caller =>
    test(s"Grant access to alice to  $caller to `proj11` with `resources/read`") {
      given Caller = caller
      for {
        _ <- aclCheck.authorizeForOr(proj1Address, read)(unauthorized).assert
        _ <- aclCheck.authorizeFor(proj1Address, read).assertEquals(true)
      } yield ()
    }
  }

  test("Prevent anonymous to access `proj11` with `resources/read`") {
    given Caller = anonymous
    for {
      _ <- aclCheck.authorizeForOr(proj1Address, read)(unauthorized).interceptEquals(unauthorized)
      _ <- aclCheck.authorizeFor(proj1Address, read).assertEquals(false)
    } yield ()
  }

  test("Prevent anonymous to access `proj11` with `resources/read`") {
    given Caller = anonymous
    for {
      _ <- aclCheck.authorizeForOr(proj1Address, read)(unauthorized).interceptEquals(unauthorized)
      _ <- aclCheck.authorizeFor(proj1Address, read).assertEquals(false)
    } yield ()
  }

  test("Grant access to alice to `proj11` with both `resources.read` and `resources/write`") {
    given Caller = alice
    aclCheck.authorizeForEveryOr(proj1Address, Set(read, write))(unauthorized).assert
  }

  test("Prevent bob to access `proj11` with both `resources.read` and `resources/write`") {
    given Caller = bob
    aclCheck.authorizeForEveryOr(proj1Address, Set(read, write))(unauthorized).interceptEquals(unauthorized)
  }

  test("Adding the missing `resources/write` now grants him the access") {
    given Caller = bob
    for {
      _ <- aclCheck.append(org1Address, bobUser -> Set(write))
      _ <- aclCheck
             .authorizeForEveryOr(proj1Address, Set(read, write))(unauthorized)
             .assert
      _ <- aclCheck.subtract(org1Address, bobUser -> Set(write))
    } yield ()
  }

  val projectValues: List[ProjectValue] = List(
    ProjectValue(proj11, read, 1),
    ProjectValue(proj11, write, 2),
    ProjectValue(proj12, read, 3),
    ProjectValue(proj11, eventsRead, 4)
  )

  test("Map and filter a list of values for the user Alice without raising an error") {
    aclCheck
      .mapFilterOrRaise[ProjectValue, Int](
        projectValues,
        v => (v.project, v.permission),
        _.index,
        _ => IO.raiseError(unauthorized)
      )(using alice)
      .assertEquals(Set(1, 2, 3, 4))
  }

  test("Map and filter a list of values for the user Alice") {
    aclCheck
      .mapFilter[ProjectValue, Int](
        projectValues,
        v => (v.project, v.permission),
        _.index
      )(using alice)
      .assertEquals(Set(1, 2, 3, 4))
  }

  test("Raise an error as bob is missing some of the acls") {
    aclCheck
      .mapFilterOrRaise[ProjectValue, Int](
        projectValues,
        v => (v.project, v.permission),
        _.index,
        _ => IO.raiseError(unauthorized)
      )(using bob)
      .interceptEquals(unauthorized)
  }

  test("Map and filter a list of values for the user Bob") {
    aclCheck
      .mapFilter[ProjectValue, Int](
        projectValues,
        v => (v.project, v.permission),
        _.index
      )(using bob)
      .assertEquals(Set(1))
  }

  val values: List[Value] = List(
    Value(read, 1),
    Value(write, 2),
    Value(read, 3),
    Value(eventsRead, 4)
  )

  test("Map and filter a list of values at a given address for the user Alice without raising an error") {
    aclCheck
      .mapFilterAtAddressOrRaise[Value, Int](
        values,
        proj12,
        _.permission,
        _.index,
        _ => IO.raiseError(unauthorized)
      )(using alice)
      .assertEquals(Set(1, 2, 3, 4))
  }

  test("Map and filter a list of values for the user Alice") {
    aclCheck
      .mapFilterAtAddress[Value, Int](
        values,
        proj11,
        _.permission,
        _.index
      )(using alice)
      .assertEquals(Set(1, 2, 3, 4))
  }

  test("Raise an error for values at a given address as bob is missing some of the acls") {
    aclCheck
      .mapFilterAtAddressOrRaise[Value, Int](
        values,
        proj11,
        _.permission,
        _.index,
        _ => IO.raiseError(unauthorized)
      )(using bob)
      .interceptEquals(unauthorized)
  }

  test("Map and filter a list of values for the user Bob") {
    aclCheck
      .mapFilterAtAddress[Value, Int](
        values,
        proj11,
        _.permission,
        _.index
      )(using bob)
      .assertEquals(Set(1, 3))
  }
}

object AclCheckSuite {

  final case class ProjectValue(project: ProjectRef, permission: Permission, index: Int)

  final case class Value(permission: Permission, index: Int)

}
