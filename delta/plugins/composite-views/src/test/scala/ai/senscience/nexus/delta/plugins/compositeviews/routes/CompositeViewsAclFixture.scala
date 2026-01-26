package ai.senscience.nexus.delta.plugins.compositeviews.routes

import ai.senscience.nexus.delta.sdk.acls.AclSimpleCheck
import ai.senscience.nexus.delta.sdk.identities.IdentitiesDummy
import ai.senscience.nexus.delta.plugins.compositeviews.model.permissions
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress
import ai.senscience.nexus.delta.sourcing.model.Identity.User
import ai.senscience.nexus.delta.sourcing.model.Label
import ai.senscience.nexus.testkit.scalatest.ce.CatsIOValues
import org.scalatest.Assertions

private[routes] trait CompositeViewsAclFixture extends CatsIOValues { self: Assertions =>

  private val realm = Label.unsafe("myrealm")
  val reader        = User("reader", realm)
  val writer        = User("writer", realm)

  val identities = IdentitiesDummy.fromUsers(reader, writer)
  val aclCheck   = AclSimpleCheck(
    (reader, AclAddress.Root, Set(permissions.read)),
    (writer, AclAddress.Root, Set(permissions.write))
  ).accepted

}
