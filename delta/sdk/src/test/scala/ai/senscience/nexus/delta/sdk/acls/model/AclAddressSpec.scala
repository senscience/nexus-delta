package ai.senscience.nexus.delta.sdk.acls.model

import ai.senscience.nexus.delta.sdk.acls.AclFixtures
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress.*
import ai.senscience.nexus.testkit.scalatest.BaseSpec
import cats.data.NonEmptyList

class AclAddressSpec extends BaseSpec with AclFixtures {

  "An ACL address" should {
    val orgAddress  = Organization(org)
    val projAddress = Project(org, proj)

    "return its string representation" in {
      val list = List(Root -> "/", orgAddress -> "/org", projAddress -> "/org/proj")
      forAll(list) { case (address, expectedString) =>
        address.string shouldEqual expectedString
      }
    }

    "return its parents" in {
      val list = List(Root -> None, orgAddress -> Some(Root), projAddress -> Some(orgAddress))
      forAll(list) { case (address, parent) =>
        address.parent shouldEqual parent
      }
    }

    "be constructed correctly from string" in {
      val list = List(Root -> "/", orgAddress -> "/org", projAddress -> "/org/proj")
      forAll(list) { case (address, string) =>
        AclAddress.fromString(string).rightValue shouldEqual address
      }
    }

    "fail to be constructed from string" in {
      val list = List("", "//", "/asd!", "/asd/a!", "/a/", s"/${genString(length = 65, 'a' to 'z')}")
      forAll(list) { string =>
        AclAddress.fromString(string).leftValue
      }
    }

    "return the correct ancestor list" in {
      val list = List(
        Root        -> NonEmptyList.one(Root),
        orgAddress  -> NonEmptyList.of(orgAddress, Root),
        projAddress -> NonEmptyList.of(projAddress, orgAddress, Root)
      )
      forAll(list) { case (address, ancestors) =>
        address.ancestors shouldEqual ancestors
      }
    }
  }
}
