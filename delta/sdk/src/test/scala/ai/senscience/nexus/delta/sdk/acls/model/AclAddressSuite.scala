package ai.senscience.nexus.delta.sdk.acls.model

import ai.senscience.nexus.delta.sdk.acls.AclFixtures
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress.*
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.data.NonEmptyList

class AclAddressSuite extends NexusSuite with AclFixtures {

  private val orgAddress  = Organization(org)
  private val projAddress = Project(org, proj)

  test("return its string representation") {
    assertEquals(Root.string, "/")
    assertEquals(orgAddress.string, "/org")
    assertEquals(projAddress.string, "/org/proj")
  }

  test("return its parents") {
    assertEquals(Root.parent, None)
    assertEquals(orgAddress.parent, Some(Root))
    assertEquals(projAddress.parent, Some(orgAddress))
  }

  test("be constructed correctly from string") {
    assertEquals(AclAddress.fromString("/").toOption, Some(Root))
    assertEquals(AclAddress.fromString("/org").toOption, Some(orgAddress))
    assertEquals(AclAddress.fromString("/org/proj").toOption, Some(projAddress))
  }

  test("fail to be constructed from string") {
    List("", "//", "/asd!", "/asd/a!", "/a/", s"/${genString(length = 65, 'a' to 'z')}").foreach { string =>
      assert(AclAddress.fromString(string).isLeft, s"Expected Left for '$string'")
    }
  }

  test("return the correct ancestor list") {
    assertEquals(Root.ancestors, NonEmptyList.one(Root))
    assertEquals(orgAddress.ancestors, NonEmptyList.of(orgAddress, Root))
    assertEquals(projAddress.ancestors, NonEmptyList.of(projAddress, orgAddress, Root))
  }
}
