package ai.senscience.nexus.delta.sdk.projects

import ai.senscience.nexus.delta.sdk.ConfigFixtures
import ai.senscience.nexus.delta.sdk.acls.model.{Acl, AclAddress, FlattenedAclStore}
import ai.senscience.nexus.delta.sdk.acls.{Acls, AclsImpl}
import ai.senscience.nexus.delta.sdk.generators.{OrganizationGen, PermissionsGen, ProjectGen}
import ai.senscience.nexus.delta.sdk.identities.model.{Caller, ServiceAccount}
import ai.senscience.nexus.delta.sdk.permissions.Permissions
import ai.senscience.nexus.delta.sourcing.model.Identity.User
import ai.senscience.nexus.delta.sourcing.model.Label
import ai.senscience.nexus.delta.sourcing.postgres.DoobieScalaTestFixture
import ai.senscience.nexus.testkit.scalatest.ce.CatsEffectSpec
import cats.effect.IO

class OwnerPermissionsScopeInitializationSpec extends CatsEffectSpec with DoobieScalaTestFixture with ConfigFixtures {

  private val saRealm: Label    = Label.unsafe("service-accounts")
  private val usersRealm: Label = Label.unsafe("users")

  private lazy val aclStore = new FlattenedAclStore(xas)

  private lazy val acls: Acls =
    AclsImpl(
      IO.pure(PermissionsGen.minimum),
      Acls.findUnknownRealms(_, Set(saRealm, usersRealm)),
      PermissionsGen.minimum,
      eventLogConfig,
      aclStore,
      xas,
      clock
    )

  private val sa: ServiceAccount = ServiceAccount(User("nexus-sa", saRealm))
  private val bob: Caller        = Caller(User("bob", usersRealm))

  "An OwnerPermissionsScopeInitialization" should {
    lazy val init = OwnerPermissionsScopeInitialization(acls, PermissionsGen.ownerPermissions, sa)

    "set the owner permissions for a newly created organization" in {
      val organization = OrganizationGen.organization(genString())
      init.onOrganizationCreation(organization, bob.subject).accepted
      val resource     = acls.fetch(organization.label).accepted
      resource.value.value shouldEqual Map(bob.subject -> PermissionsGen.ownerPermissions)
      resource.rev shouldEqual 1L
      resource.createdBy shouldEqual sa.caller.subject
    }

    "not set owner permissions if acls are already defined for an org" in {
      val organization = OrganizationGen.organization(genString())
      acls
        .append(Acl(organization.label, bob.subject -> Set(Permissions.resources.read)), 0)(
          sa.caller.subject
        )
        .accepted
      init.onOrganizationCreation(organization, bob.subject).accepted
      val resource     = acls.fetch(organization.label).accepted
      resource.value.value shouldEqual Map(bob.subject -> Set(Permissions.resources.read))
      resource.rev shouldEqual 1L
      resource.createdBy shouldEqual sa.caller.subject
    }

    "set the owner permissions for a newly created project" in {
      val project  = ProjectGen.project(genString(), genString())
      init.onProjectCreation(project.ref, bob.subject).accepted
      val resource = acls.fetch(AclAddress.Project(project.ref)).accepted
      resource.value.value shouldEqual Map(bob.subject -> PermissionsGen.ownerPermissions)
      resource.rev shouldEqual 1L
      resource.createdBy shouldEqual sa.caller.subject
    }

    "not set owner permissions if acls are already defined for a project" in {
      val project  = ProjectGen.project(genString(), genString())
      acls
        .append(Acl(AclAddress.Project(project.ref), bob.subject -> Set(Permissions.resources.read)), 0)(
          sa.caller.subject
        )
        .accepted
      init.onProjectCreation(project.ref, bob.subject).accepted
      val resource = acls.fetch(AclAddress.Project(project.ref)).accepted
      resource.value.value shouldEqual Map(bob.subject -> Set(Permissions.resources.read))
      resource.rev shouldEqual 1L
      resource.createdBy shouldEqual sa.caller.subject
    }
  }

}
