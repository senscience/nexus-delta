package ai.senscience.nexus.delta.sdk.acls

import ai.senscience.nexus.delta.sdk.acls.model.Acl
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress.{Organization, Root}
import ai.senscience.nexus.delta.sdk.permissions.model.Permission
import ai.senscience.nexus.delta.sourcing.model.Label
import ai.senscience.nexus.testkit.scalatest.BaseSpec

class AclSpec extends BaseSpec with AclFixtures {

  "An Access Control List" should {

    val org = Organization(Label.unsafe("org"))

    "add another ACL" in {
      userRW_groupX(Root) ++ userR_groupX(Root) shouldEqual userRW_groupX(Root)
      userRW_groupX(Root) ++ anonR(Root) shouldEqual Acl(Root, subject -> Set(r, w), group -> Set(x), anon -> Set(r))
      userRW_groupX(Root) ++ groupR(Root) shouldEqual Acl(Root, subject -> Set(r, w), group -> Set(r, x))
      userRW_groupX(Root) ++ groupR(org) shouldEqual userRW_groupX(Root)
    }

    "subtract an ACL" in {
      userRW_groupX(Root) -- groupR(Root) shouldEqual userRW_groupX(Root)
      userRW_groupX(Root) -- anonR(Root) shouldEqual userRW_groupX(Root)
      userRW_groupX(Root) -- userR_groupX(Root) shouldEqual Acl(Root, subject -> Set(w))
      userRW_groupX(Root) -- userR_groupX(org) shouldEqual userRW_groupX(Root)
    }

    "return all its permissions" in {
      userRW_groupX(Root).permissions shouldEqual Set(r, w, x)
    }

    "check if it is empty" in {
      Acl(Root, anon -> Set.empty[Permission], subject -> Set.empty[Permission]).isEmpty shouldEqual true
      Acl(Root).isEmpty shouldEqual true
      userRW_groupX(Root).isEmpty shouldEqual false
    }

    "check if it has some empty permissions" in {
      Acl(Root, subject -> Set.empty[Permission]).hasEmptyPermissions shouldEqual true
      Acl(Root, subject -> Set.empty[Permission], anon -> Set(r)).hasEmptyPermissions shouldEqual true
      userRW_groupX(Root).hasEmptyPermissions shouldEqual false
    }

    "remove empty permissions" in {
      Acl(Root, subject -> Set.empty[Permission]).removeEmpty() shouldEqual Acl(Root)
      Acl(Root, subject -> Set(), anon -> Set(r)).removeEmpty() shouldEqual Acl(Root, anon -> Set(r))
      userRW_groupX(Root).removeEmpty() shouldEqual userRW_groupX(Root)
    }

    "be filtered" in {
      userRW_groupX(Root).filter(Set(subject, anon)) shouldEqual Acl(Root, subject -> Set(r, w))
    }

    "check for permissions" in {
      userRW_groupX(Root).hasPermission(Set(subject, anon), r) shouldEqual true
      userRW_groupX(Root).hasPermission(Set(subject, anon), x) shouldEqual false
      userRW_groupX(Root).hasPermission(Set(anon), r) shouldEqual false
    }

  }
}
