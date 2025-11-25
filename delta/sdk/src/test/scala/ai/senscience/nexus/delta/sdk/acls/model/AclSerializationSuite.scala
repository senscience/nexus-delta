package ai.senscience.nexus.delta.sdk.acls.model

import ai.senscience.nexus.delta.sdk.SerializationSuite
import ai.senscience.nexus.delta.sdk.acls.model.AclEvent.{AclAppended, AclDeleted, AclReplaced, AclSubtracted}
import ai.senscience.nexus.delta.sdk.permissions.model.Permission
import ai.senscience.nexus.delta.sourcing.model.Identity.{Anonymous, Authenticated, Group, Role, Subject, User}
import ai.senscience.nexus.delta.sourcing.model.{Identity, Label}

import java.time.Instant

class AclSerializationSuite extends SerializationSuite {

  val instant: Instant = Instant.EPOCH
  val rev: Int         = 1

  val realm: Label             = Label.unsafe("myrealm")
  val subject: Subject         = User("username", realm)
  val anonymous: Subject       = Anonymous
  val role: Identity           = Role("role", realm)
  val group: Identity          = Group("group", realm)
  val authenticated: Identity  = Authenticated(realm)
  val permSet: Set[Permission] = Set(Permission.unsafe("my/perm"))

  val root: AclAddress                    = AclAddress.Root
  val orgAddress: AclAddress.Organization = AclAddress.Organization(Label.unsafe("myorg"))
  val projAddress: AclAddress.Project     = AclAddress.Project(Label.unsafe("myorg"), Label.unsafe("myproj"))

  private def acl(address: AclAddress): Acl =
    Acl(address, Anonymous -> permSet, authenticated -> permSet, role -> permSet, group -> permSet, subject -> permSet)

  private val aclsMapping = Map(
    AclAppended(acl(root), rev, instant, subject)         -> loadDatabaseEvents("acls", "acl-appended.json"),
    AclSubtracted(acl(orgAddress), rev, instant, subject) -> loadDatabaseEvents("acls", "acl-subtracted.json"),
    AclReplaced(acl(projAddress), rev, instant, subject)  -> loadDatabaseEvents("acls", "acl-replaced.json"),
    AclDeleted(projAddress, rev, instant, anonymous)      -> loadDatabaseEvents("acls", "acl-deleted.json")
  )

  aclsMapping.foreach { case (event, database) =>
    test(s"Correctly serialize event '${event.getClass.getSimpleName}'") {
      assertOutputIgnoreOrder(AclEvent.serializer, event, database)
    }

    test(s"Correctly deserialize event '${event.getClass.getSimpleName}'") {
      assertEquals(AclEvent.serializer.codec.decodeJson(database), Right(event))
    }
  }

  private val state = AclState(
    acl(projAddress),
    rev = rev,
    createdAt = instant,
    createdBy = subject,
    updatedAt = instant,
    updatedBy = subject
  )

  private val aclState = jsonContentOf("acls/acl-state.json")

  test("Correctly serialize an AclState") {
    assertOutputIgnoreOrder(AclState.serializer, state, aclState)
  }

  test("Correctly deserialize an AclState") {
    assertEquals(AclState.serializer.codec.decodeJson(aclState), Right(state))
  }

}
