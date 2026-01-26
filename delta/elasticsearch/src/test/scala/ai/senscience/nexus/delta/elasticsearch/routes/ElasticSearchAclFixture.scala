package ai.senscience.nexus.delta.elasticsearch.routes

import ai.senscience.nexus.delta.sdk.acls.AclSimpleCheck
import ai.senscience.nexus.delta.sdk.identities.IdentitiesDummy
import ai.senscience.nexus.delta.sourcing.model.Identity.User
import ai.senscience.nexus.delta.sourcing.model.Label
import ai.senscience.nexus.testkit.scalatest.ce.CatsIOValues
import org.scalatest.Assertions

private[routes] trait ElasticSearchAclFixture extends CatsIOValues { self: Assertions =>

  private val realm: Label = Label.unsafe("wonderland")

  val reader = User("reader", realm)
  val writer = User("writer", realm)
  val admin  = User("admin", realm)

  val identities = IdentitiesDummy.fromUsers(reader, writer, admin)

  val aclCheck: AclSimpleCheck = AclSimpleCheck().accepted

}
