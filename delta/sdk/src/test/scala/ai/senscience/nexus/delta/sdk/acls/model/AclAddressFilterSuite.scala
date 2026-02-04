package ai.senscience.nexus.delta.sdk.acls.model

import ai.senscience.nexus.delta.sdk.acls.AclFixtures
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress.*
import ai.senscience.nexus.delta.sdk.acls.model.AclAddressFilter.{AnyOrganization, AnyOrganizationAnyProject, AnyProject}
import ai.senscience.nexus.delta.sourcing.model.Label
import ai.senscience.nexus.testkit.mu.NexusSuite

class AclAddressFilterSuite extends NexusSuite with AclFixtures {

  private val org2          = Label.unsafe("org2")
  private val proj2         = Label.unsafe("proj2")
  private val orgAddress    = Organization(org)
  private val org2Address   = Organization(org2)
  private val projAddress   = Project(org, proj)
  private val proj12Address = Project(org, proj2)
  private val proj22Address = Project(org2, proj2)
  private val anyFilter     = AnyOrganizationAnyProject(false)
  private val anyOrgFilter  = AnyOrganization(false)

  test("return its string representation") {
    assertEquals(anyFilter.string, "/*/*")
    assertEquals(anyOrgFilter.string, "/*")
    assertEquals(AnyProject(org, withAncestors = false).string, "/org/*")
  }

  test("match an address") {
    List(
      anyOrgFilter                           -> orgAddress,
      AnyProject(org, withAncestors = false) -> projAddress,
      AnyProject(org, withAncestors = false) -> proj12Address,
      anyFilter                              -> projAddress,
      anyFilter                              -> proj12Address,
      anyFilter                              -> proj22Address
    ).foreach { case (filter, address) =>
      assert(filter.matches(address), s"Expected $filter to match $address")
    }
  }

  test("not match an address") {
    List(
      anyOrgFilter                           -> Root,
      anyOrgFilter                           -> projAddress,
      AnyProject(org, withAncestors = false) -> Root,
      AnyProject(org, withAncestors = false) -> orgAddress,
      AnyProject(org, withAncestors = false) -> proj22Address,
      anyFilter                              -> Root,
      anyFilter                              -> orgAddress
    ).foreach { case (filter, address) =>
      assert(!filter.matches(address), s"Expected $filter to NOT match $address")
    }
  }

  test("match an address and its ancestors") {
    val anyFilterWithAncestors    = AnyOrganizationAnyProject(true)
    val anyOrgFilterWithAncestors = AnyOrganization(true)

    List(
      anyOrgFilterWithAncestors             -> Root,
      anyOrgFilterWithAncestors             -> orgAddress,
      AnyProject(org, withAncestors = true) -> Root,
      AnyProject(org, withAncestors = true) -> orgAddress,
      AnyProject(org, withAncestors = true) -> projAddress,
      AnyProject(org, withAncestors = true) -> proj12Address,
      anyFilterWithAncestors                -> Root,
      anyFilterWithAncestors                -> orgAddress,
      anyFilterWithAncestors                -> org2Address,
      anyFilterWithAncestors                -> projAddress,
      anyFilterWithAncestors                -> proj12Address,
      anyFilterWithAncestors                -> proj22Address
    ).foreach { case (filter, address) =>
      assert(filter.matches(address), s"Expected $filter to match $address (with ancestors)")
    }
  }

  test("not match an address nor its ancestors") {
    List(
      anyOrgFilter           -> projAddress,
      AnyProject(org, false) -> org2Address,
      AnyProject(org, false) -> proj22Address
    ).foreach { case (filter, address) =>
      assert(!filter.matches(address), s"Expected $filter to NOT match $address")
    }
  }
}
