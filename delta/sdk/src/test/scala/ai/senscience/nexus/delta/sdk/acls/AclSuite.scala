package ai.senscience.nexus.delta.sdk.acls

import ai.senscience.nexus.delta.sdk.acls.model.Acl
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress.{Organization, Root}
import ai.senscience.nexus.delta.sdk.permissions.model.Permission
import ai.senscience.nexus.delta.sourcing.model.Label
import ai.senscience.nexus.testkit.mu.NexusSuite

class AclSuite extends NexusSuite with AclFixtures {

  private val orgAddress = Organization(Label.unsafe("org"))

  test("add another ACL") {
    assertEquals(userRW_groupX(Root) ++ userR_groupX(Root), userRW_groupX(Root))
    assertEquals(userRW_groupX(Root) ++ anonR(Root), Acl(Root, subject -> Set(r, w), group -> Set(x), anon -> Set(r)))
    assertEquals(userRW_groupX(Root) ++ groupR(Root), Acl(Root, subject -> Set(r, w), group -> Set(r, x)))
    assertEquals(userRW_groupX(Root) ++ groupR(orgAddress), userRW_groupX(Root))
  }

  test("subtract an ACL") {
    assertEquals(userRW_groupX(Root) -- groupR(Root), userRW_groupX(Root))
    assertEquals(userRW_groupX(Root) -- anonR(Root), userRW_groupX(Root))
    assertEquals(userRW_groupX(Root) -- userR_groupX(Root), Acl(Root, subject -> Set(w)))
    assertEquals(userRW_groupX(Root) -- userR_groupX(orgAddress), userRW_groupX(Root))
  }

  test("return all its permissions") {
    assertEquals(userRW_groupX(Root).permissions, Set(r, w, x))
  }

  test("check if it is empty") {
    assertEquals(Acl(Root, anon -> Set.empty[Permission], subject -> Set.empty[Permission]).isEmpty, true)
    assertEquals(Acl(Root).isEmpty, true)
    assertEquals(userRW_groupX(Root).isEmpty, false)
  }

  test("check if it has some empty permissions") {
    assertEquals(Acl(Root, subject -> Set.empty[Permission]).hasEmptyPermissions, true)
    assertEquals(Acl(Root, subject -> Set.empty[Permission], anon -> Set(r)).hasEmptyPermissions, true)
    assertEquals(userRW_groupX(Root).hasEmptyPermissions, false)
  }

  test("remove empty permissions") {
    assertEquals(Acl(Root, subject -> Set.empty[Permission]).removeEmpty(), Acl(Root))
    assertEquals(Acl(Root, subject -> Set(), anon -> Set(r)).removeEmpty(), Acl(Root, anon -> Set(r)))
    assertEquals(userRW_groupX(Root).removeEmpty(), userRW_groupX(Root))
  }

  test("be filtered") {
    assertEquals(userRW_groupX(Root).filter(Set(subject, anon)), Acl(Root, subject -> Set(r, w)))
  }

  test("check for permissions") {
    assertEquals(userRW_groupX(Root).hasPermission(Set(subject, anon), r), true)
    assertEquals(userRW_groupX(Root).hasPermission(Set(subject, anon), x), false)
    assertEquals(userRW_groupX(Root).hasPermission(Set(anon), r), false)
  }
}
