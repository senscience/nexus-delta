package ai.senscience.nexus.delta.sdk.projects

import ai.senscience.nexus.delta.sdk.ConfigFixtures
import ai.senscience.nexus.delta.sdk.acls.model.{Acl, AclAddress, FlattenedAclStore}
import ai.senscience.nexus.delta.sdk.acls.{Acls, AclsImpl}
import ai.senscience.nexus.delta.sdk.generators.{OrganizationGen, PermissionsGen, ProjectGen}
import ai.senscience.nexus.delta.sdk.identities.model.{Caller, ServiceAccount}
import ai.senscience.nexus.delta.sdk.permissions.Permissions
import ai.senscience.nexus.delta.sdk.permissions.model.Permission
import ai.senscience.nexus.delta.sourcing.model.Identity
import ai.senscience.nexus.delta.sourcing.model.Identity.{Subject, User}
import ai.senscience.nexus.delta.sourcing.model.Label
import ai.senscience.nexus.delta.sourcing.postgres.Doobie
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.effect.IO
import munit.{AnyFixture, Location}

class OwnerPermissionsScopeInitializationSuite extends NexusSuite with Doobie.Fixture with ConfigFixtures {

  override def munitFixtures: Seq[AnyFixture[?]] = List(doobie)

  private val saRealm: Label    = Label.unsafe("service-accounts")
  private val usersRealm: Label = Label.unsafe("users")

  private lazy val xas = doobie()

  private lazy val acls: Acls =
    AclsImpl(
      IO.pure(PermissionsGen.minimum),
      Acls.findUnknownRealms(_, Set(saRealm, usersRealm)),
      PermissionsGen.minimum,
      eventLogConfig,
      new FlattenedAclStore(xas),
      xas,
      clock
    )

  private val sa: ServiceAccount = ServiceAccount(User("nexus-sa", saRealm))
  private val bob: Caller        = Caller(User("bob", usersRealm))

  private val readOnly: Set[Permission] = Set(Permissions.resources.read)

  private lazy val init = OwnerPermissionsScopeInitialization(acls, PermissionsGen.ownerPermissions, sa)

  private def assertAcl(address: AclAddress, rev: Int, createdBy: Subject, acl: (Identity, Set[Permission]))(using
      Location
  ) = {
    val aclMap = Map(acl._1 -> acl._2)
    acls.fetch(address).map { acl =>
      assertEquals(acl.value.value, aclMap)
      assertEquals(acl.rev, rev)
      assertEquals(acl.createdBy, createdBy)
    }
  }

  test("Set the owner permissions for a newly created organization") {
    val organization = OrganizationGen.organization(genString())
    init.onOrganizationCreation(organization, bob.subject) >>
      assertAcl(organization.label, 1, sa.caller.subject, bob.subject -> PermissionsGen.ownerPermissions)
  }

  test("Not set owner permissions if acls are already defined for an org") {
    val organization = OrganizationGen.organization(genString())
    acls.append(Acl(organization.label, bob.subject -> readOnly), 0)(using sa.caller.subject) >>
      init.onOrganizationCreation(organization, bob.subject) >>
      assertAcl(organization.label, 1, sa.caller.subject, bob.subject -> readOnly)
  }

  test("Set the owner permissions for a newly created project") {
    val project = ProjectGen.project(genString(), genString())
    init.onProjectCreation(project.ref, bob.subject) >>
      assertAcl(project.ref, 1, sa.caller.subject, bob.subject -> PermissionsGen.ownerPermissions)
  }

  test("Not set owner permissions if acls are already defined for a project") {
    val project = ProjectGen.project(genString(), genString())
    acls.append(Acl(project.ref, bob.subject -> readOnly), 0)(using sa.caller.subject) >>
      init.onProjectCreation(project.ref, bob.subject) >>
      assertAcl(project.ref, 1, sa.caller.subject, bob.subject -> readOnly)
  }
}
