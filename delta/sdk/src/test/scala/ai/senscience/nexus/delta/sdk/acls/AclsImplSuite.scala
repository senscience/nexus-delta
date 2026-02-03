package ai.senscience.nexus.delta.sdk.acls

import ai.senscience.nexus.delta.sdk.ConfigFixtures
import ai.senscience.nexus.delta.sdk.acls.model.*
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress.Organization
import ai.senscience.nexus.delta.sdk.acls.model.AclAddressFilter.{AnyOrganization, AnyOrganizationAnyProject, AnyProject}
import ai.senscience.nexus.delta.sdk.acls.model.AclRejection.*
import ai.senscience.nexus.delta.sdk.generators.AclGen.resourceFor
import ai.senscience.nexus.delta.sdk.generators.PermissionsGen
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.permissions.model.Permission
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.{Label, ProjectRef}
import ai.senscience.nexus.delta.sourcing.postgres.Doobie
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.effect.IO
import doobie.syntax.all.*
import munit.AnyFixture

class AclsImplSuite extends NexusSuite with Doobie.Fixture with AclFixtures with ConfigFixtures {

  override def munitFixtures: Seq[AnyFixture[?]] = List(doobie)

  private given Subject = subject
  private given Caller  = Caller(subject)

  private val org2: Label         = Label.unsafe("org2")
  private val orgTarget           = AclAddress.Organization(org)
  private val org2Target          = AclAddress.Organization(org2)
  private val projectTarget       = AclAddress.Project(org, proj)
  private val any                 = AnyOrganizationAnyProject(false)
  private val anyWithAncestors    = AnyOrganizationAnyProject(true)
  private val anyOrg              = AnyOrganization(false)
  private val anyOrgWithAncestors = AnyOrganization(true)

  private val minimumPermissions: Set[Permission] = PermissionsGen.minimum

  private lazy val xas        = doobie()
  private lazy val aclStore   = new FlattenedAclStore(xas)
  private lazy val acls: Acls = AclsImpl(
    IO.pure(minimumPermissions),
    Acls.findUnknownRealms(_, Set(realm, realm2)),
    minimumPermissions,
    eventLogConfig,
    aclStore,
    xas,
    clock
  )

  test("Return the full permissions for Anonymous if no permissions are defined") {
    val expected: AclCollection = AclCollection(AclState.initial(minimumPermissions).toResource)
    for {
      _ <- acls.fetchWithAncestors(projectTarget).assertEquals(expected)
      _ <- acls.fetchWithAncestors(orgTarget).assertEquals(expected)
      _ <- acls.fetchWithAncestors(AclAddress.Root).assertEquals(expected)
    } yield ()
  }

  test("Return false on `/` as no acl is set yet") {
    acls.isRootAclSet.assertEquals(false)
  }

  test("Append an ACL") {
    acls.append(userR(AclAddress.Root), 0).assertEquals(resourceFor(userR(AclAddress.Root), 1, subject))
  }

  test("Return true on `/` now that an acl has been appended") {
    acls.isRootAclSet.assertEquals(true)
  }

  test("Should not return permissions for Anonymous after a new revision was recorded on Root") {
    val expected = AclCollection(resourceFor(userR(AclAddress.Root), 1, subject))
    for {
      _ <- acls.fetchWithAncestors(projectTarget).assertEquals(expected)
      _ <- acls.fetchWithAncestors(orgTarget).assertEquals(expected)
      _ <- acls.fetchWithAncestors(AclAddress.Root).assertEquals(expected)
    } yield ()
  }

  test("Replace an ACL") {
    val expected = resourceFor(userR_groupX(AclAddress.Root), 2, subject)
    acls.replace(userR_groupX(AclAddress.Root), 1).assertEquals(expected)
  }

  test("Subtract an ACL") {
    val expected = resourceFor(userR(AclAddress.Root), 3, subject)
    acls.subtract(groupX(AclAddress.Root), 2).assertEquals(expected)
  }

  test("Delete an ACL") {
    val expected = resourceFor(Acl(AclAddress.Root), 4, subject)
    acls.delete(AclAddress.Root, 3).assertEquals(expected)
  }

  test("Fetch an ACL") {
    for {
      _ <- acls.replace(userR_groupX(orgTarget), 0)
      _ <- acls.fetch(orgTarget).assertEquals(resourceFor(userR_groupX(orgTarget), 1, subject))
    } yield ()
  }

  test("Fetch an ACL containing only caller identities") {
    acls.fetchSelf(orgTarget).assertEquals(resourceFor(userR(orgTarget), 1, subject))
  }

  test("Fetch an ACL data") {
    for {
      _ <- acls.fetchAcl(orgTarget).assertEquals(userR_groupX(orgTarget))
      _ <- acls.fetchSelfAcl(orgTarget).assertEquals(userR(orgTarget))
      _ <- acls.fetchAcl(projectTarget).assertEquals(Acl(projectTarget))
    } yield ()
  }

  test("Fetch an ACL at specific revision") {
    for {
      _ <- acls.append(userW(orgTarget), 1)
      _ <- acls.fetchAt(orgTarget, 2).assertEquals(resourceFor(userRW_groupX(orgTarget), 2, subject))
      _ <- acls.fetchAt(orgTarget, 1).assertEquals(resourceFor(userR_groupX(orgTarget), 1, subject))
    } yield ()
  }

  test("Fetch an ACL at specific revision containing only caller identities") {
    acls.fetchSelfAt(orgTarget, 1).assertEquals(resourceFor(userR(orgTarget), 1, subject))
  }

  test("Fail fetching a non existing acl") {
    val targetNotExist = Organization(Label.unsafe("other"))
    acls.fetch(targetNotExist).intercept[AclNotFound]
  }

  test("Fail fetching a non existing acl at specific revision") {
    val targetNotExist = Organization(Label.unsafe("other"))
    acls.fetchAt(targetNotExist, 1).intercept[AclNotFound]
  }

  test("List ACLs") {
    val expected = AclCollection(resourceFor(anonR(projectTarget), 1, subject))
    for {
      _ <- acls.append(groupR(AclAddress.Root), 4)
      _ <- acls.append(anonR(projectTarget), 0)
      _ <- acls.list(any).assertEquals(expected)
      _ <- acls.list(AnyProject(org, withAncestors = false)).assertEquals(expected)
    } yield ()
  }

  test("List ACLs containing only caller identities") {
    acls.listSelf(anyOrg).assertEquals(AclCollection(resourceFor(userRW(orgTarget), 2, subject)))
  }

  test("List ACLs containing ancestors") {
    val rootAcl    = resourceFor(groupR(AclAddress.Root), 5, subject)
    val orgAcl     = resourceFor(userRW_groupX(orgTarget), 2, subject)
    val org2Acl    = resourceFor(userRW(org2Target), 1, subject)
    val projectAcl = resourceFor(anonR(projectTarget), 1, subject)

    for {
      _ <- acls.append(userRW(org2Target), 0)
      _ <- acls.list(anyWithAncestors).assertEquals(AclCollection(rootAcl, orgAcl, org2Acl, projectAcl))
      _ <- acls.list(AnyProject(org, withAncestors = true)).assertEquals(AclCollection(rootAcl, orgAcl, projectAcl))
      _ <- acls.list(anyOrgWithAncestors).assertEquals(AclCollection(rootAcl, orgAcl, org2Acl))
    } yield ()
  }

  test("List ACLs containing ancestors and caller identities") {
    val expected = AclCollection(
      resourceFor(Acl(AclAddress.Root), 5, subject),
      resourceFor(userRW(orgTarget), 2, subject),
      resourceFor(userRW(org2Target), 1, subject)
    )
    acls.listSelf(anyOrgWithAncestors).assertEquals(expected)
  }

  test("Fetch ACLs containing ancestors") {
    val expected = AclCollection(
      resourceFor(groupR(AclAddress.Root), 5, subject),
      resourceFor(userRW_groupX(orgTarget), 2, subject),
      resourceFor(anonR(projectTarget), 1, subject)
    )
    acls.fetchWithAncestors(projectTarget).assertEquals(expected)
  }

  test("Fail to fetch an ACL on nonexistent revision") {
    acls.fetchAt(orgTarget, 10).interceptEquals(RevisionNotFound(10, 2))
  }

  test("Fail to append an ACL already appended") {
    acls.append(userRW(org2Target), 1).intercept[NothingToBeUpdated]
  }

  test("Fail to subtract an ACL with permissions that do not exist") {
    acls.subtract(anonR(org2Target), 1).intercept[NothingToBeUpdated]
  }

  test("Fail to replace an ACL containing empty permissions") {
    val aclWithEmptyPerms = Acl(org2Target, subject -> Set(r), group -> Set.empty)
    acls.replace(aclWithEmptyPerms, 1).intercept[AclCannotContainEmptyPermissionCollection]
  }

  test("Fail to append an ACL containing empty permissions") {
    val aclWithEmptyPerms = Acl(org2Target, subject -> Set(r), group -> Set.empty)
    acls.append(aclWithEmptyPerms, 1).intercept[AclCannotContainEmptyPermissionCollection]
  }

  test("Fail to subtract an ACL containing empty permissions") {
    val aclWithEmptyPerms = Acl(org2Target, subject -> Set(r), group -> Set.empty)
    acls.subtract(aclWithEmptyPerms, 1).intercept[AclCannotContainEmptyPermissionCollection]
  }

  test("Fail to delete an ACL already deleted") {
    for {
      _ <- acls.delete(org2Target, 1)
      _ <- acls.delete(org2Target, 2).intercept[AclIsEmpty]
    } yield ()
  }

  test("Fail to subtract an ACL that does not exist") {
    val targetNotExist = Organization(Label.unsafe("other"))
    acls.subtract(anonR(targetNotExist), 0).intercept[AclNotFound]
  }

  test("Fail to delete an ACL that does not exist") {
    val targetNotExist = Organization(Label.unsafe("other"))
    acls.delete(targetNotExist, 0).intercept[AclNotFound]
  }

  test("Fail to replace an ACL containing invalid permissions") {
    val aclWithInvalidPerms = Acl(org2Target, subject -> Set(r), group -> Set(Permission.unsafe("invalid")))
    acls.replace(aclWithInvalidPerms, 2).intercept[UnknownPermissions]
  }

  test("Fail to append an ACL containing invalid permissions") {
    val aclWithInvalidPerms = Acl(org2Target, subject -> Set(r), group -> Set(Permission.unsafe("invalid")))
    acls.append(aclWithInvalidPerms, 2).intercept[UnknownPermissions]
  }

  test("Fail to subtract an ACL containing invalid permissions") {
    val aclWithInvalidPerms = Acl(orgTarget, subject -> Set(r), group -> Set(Permission.unsafe("invalid")))
    acls.subtract(aclWithInvalidPerms, 2).intercept[UnknownPermissions]
  }

  test("Subtract an ACL correctly") {
    for {
      _ <- acls.replace(userRW(AclAddress.Root), 5)
      _ <- acls.subtract(userW(AclAddress.Root), 6)
      _ <- acls.fetch(AclAddress.Root).assertEquals(resourceFor(userR(AclAddress.Root), 7, subject))
    } yield ()
  }

  test("Should delete the entry for a project") {
    val project      = ProjectRef.unsafe("org", "to_delete")
    val deletionTask = Acls.projectDeletionTask(acls)
    for {
      _ <- acls.append(userR(project), 0)
      _ <- deletionTask(project)
      _ <- acls.fetch(project).intercept[AclNotFound]
    } yield ()
  }

  test("Should replay the acl projection which should match the previous one") {
    def count = sql"SELECT count(*) FROM flattened_acls".query[Int].unique.transact(xas.read)
    for {
      beforeCount <- count
      _           <- AclsImpl.replayAclProjection(acls, aclStore, xas)
      afterCount  <- count
      _            = assertEquals(afterCount, beforeCount)
    } yield ()
  }
}
