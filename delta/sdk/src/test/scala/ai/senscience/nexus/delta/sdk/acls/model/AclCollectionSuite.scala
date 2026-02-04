package ai.senscience.nexus.delta.sdk.acls.model

import ai.senscience.nexus.delta.sdk.AclResource
import ai.senscience.nexus.delta.sdk.acls.AclFixtures
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress.{Organization, Project, Root}
import ai.senscience.nexus.delta.sdk.acls.model.AclAddressFilter.{AnyOrganization, AnyOrganizationAnyProject, AnyProject}
import ai.senscience.nexus.delta.sdk.generators.AclGen
import ai.senscience.nexus.delta.sdk.permissions.model.Permission
import ai.senscience.nexus.delta.sourcing.model.Label
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.syntax.functor.*

class AclCollectionSuite extends NexusSuite with AclFixtures {

  private val orgAddress  = Organization(org)
  private val projAddress = Project(org, proj)

  private def aclResource(address: AclAddress): AclResource  = AclGen.resourceFor(userRW_groupX(address), 1, subject)
  private def aclResource2(address: AclAddress): AclResource = AclGen.resourceFor(groupR(address), 2, subject)
  private def aclResource3(address: AclAddress): AclResource = AclGen.resourceFor(groupX(address), 3, subject)

  test("be merged with other ACL collection") {
    val acls1    = AclCollection(aclResource(Root))
    val acl3Proj = aclResource3(projAddress)
    val acls2    = AclCollection(aclResource2(Root), acl3Proj)
    val expected = AclCollection(aclResource2(Root).as(Acl(Root, subject -> Set(r, w), group -> Set(r, x))), acl3Proj)

    assertEquals(acls1 ++ acls2, expected)
    assertEquals(acls1 + aclResource2(Root) + acl3Proj, expected)
  }

  test("filter identities") {
    val collection = AclCollection(aclResource(projAddress), aclResource2(orgAddress), aclResource3(Root))
    val expected   = AclCollection(
      aclResource3(Root).as(Acl(Root)),
      aclResource2(orgAddress).as(Acl(orgAddress)),
      aclResource(projAddress).as(Acl(projAddress, subject -> Set(r, w)))
    )

    assertEquals(collection.filter(Set(subject)), expected)
    assertEquals(collection.filter(Set(subject, group)), collection)
  }

  test("filter identities by permission") {
    val collection = AclCollection(aclResource(projAddress), aclResource2(orgAddress), aclResource3(Root))
    assertEquals(collection.filterByPermission(Set(subject), r), AclCollection.empty + aclResource(projAddress))
  }

  test("subtract an ACL") {
    assertEquals(
      AclCollection(aclResource(Root)) - aclResource3(Root),
      AclCollection(aclResource3(Root).as(userRW(Root)))
    )
    assertEquals(AclCollection(aclResource(Root)) - aclResource(Root), AclCollection.empty)
  }

  test("subtract an address") {
    val collection = AclCollection(aclResource2(Root), aclResource3(projAddress))
    assertEquals(collection - projAddress, AclCollection(aclResource2(Root)))
  }

  test("remove empty ACL") {
    val proj2      = Label.unsafe("proj2")
    val collection = AclCollection(
      aclResource(projAddress),
      aclResource(Root).as(Acl(Root)),
      aclResource(Project(org, proj2)).as(Acl(Project(org, proj2), subject -> Set(r, w), group -> Set.empty)),
      aclResource(orgAddress).as(Acl(orgAddress, subject -> Set.empty[Permission], group -> Set.empty[Permission]))
    )

    assertEquals(
      collection.removeEmpty(),
      AclCollection(
        aclResource(projAddress),
        aclResource(Project(org, proj2)).as(Acl(Project(org, proj2), subject -> Set(r, w)))
      )
    )
  }

  test("check for matching identities and permission on a Root address") {
    val collection = AclCollection(aclResource(Root))

    List(projAddress, orgAddress, Root).foreach { address =>
      assert(collection.exists(Set(subject, anon), r, address), s"Expected exists for $address with subject")
      assert(!collection.exists(Set(anon), r, address), s"Expected NOT exists for $address with anon only")
      assert(!collection.exists(Set(subject, anon), x, address), s"Expected NOT exists for $address with permission x")
    }
  }

  test("check for matching identities and permission on a Project address") {
    val collection = AclCollection(aclResource(projAddress): AclResource)
    val proj2      = Label.unsafe("proj2")

    List(Project(org, proj2), orgAddress, Root).foreach { address =>
      assert(!collection.exists(Set(subject, anon), r, address), s"Expected NOT exists for $address")
    }
    assert(collection.exists(Set(subject, anon), r, projAddress))
    assert(!collection.exists(Set(anon), r, projAddress))
    assert(!collection.exists(Set(subject, anon), x, projAddress))
  }

  test("fetch ACLs from given filter") {
    val org2          = Label.unsafe("org2")
    val proj2         = Label.unsafe("proj2")
    val org2Address   = Organization(org2)
    val proj12Address = Project(org, proj2)
    val proj22Address = Project(org2, proj2)
    val any           = AnyOrganizationAnyProject(false)
    val anyOrg        = AnyOrganization(false)

    val orgAcl: AclResource    = aclResource2(orgAddress)
    val org2Acl: AclResource   = aclResource3(org2Address)
    val projAcl: AclResource   = aclResource2(projAddress)
    val proj12Acl: AclResource = aclResource3(proj12Address)
    val proj22Acl: AclResource = aclResource(proj22Address)
    val collection             = AclCollection(aclResource(Root), orgAcl, org2Acl, projAcl, proj12Acl, proj22Acl)

    assertEquals(collection.fetch(any), AclCollection(projAcl, proj12Acl, proj22Acl))
    assertEquals(collection.fetch(AnyProject(org, withAncestors = false)), AclCollection(projAcl, proj12Acl))
    assertEquals(collection.fetch(anyOrg), AclCollection(orgAcl, org2Acl))
  }

  test("fetch ACLs from given filter including the ancestor addresses") {
    val org2          = Label.unsafe("org2")
    val proj2         = Label.unsafe("proj2")
    val org2Address   = Organization(org2)
    val proj12Address = Project(org, proj2)
    val proj22Address = Project(org2, proj2)
    val any           = AnyOrganizationAnyProject(true)
    val anyOrg        = AnyOrganization(true)

    val orgAcl: AclResource    = aclResource2(orgAddress)
    val org2Acl: AclResource   = aclResource3(org2Address)
    val projAcl: AclResource   = aclResource2(projAddress)
    val proj12Acl: AclResource = aclResource3(proj12Address)
    val proj22Acl: AclResource = aclResource(proj22Address)
    val collection             = AclCollection(aclResource(Root), orgAcl, org2Acl, projAcl, proj12Acl, proj22Acl)

    assertEquals(collection.fetch(any), collection)
    assertEquals(
      collection.fetch(AnyProject(org, withAncestors = true)),
      AclCollection(aclResource(Root), orgAcl, projAcl, proj12Acl)
    )
    assertEquals(collection.fetch(anyOrg), AclCollection(aclResource(Root), orgAcl, org2Acl))
  }
}
