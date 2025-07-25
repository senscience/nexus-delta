package ai.senscience.nexus.delta.plugins.compositeviews.routes

import ai.senscience.nexus.delta.plugins.compositeviews.{CompositeViewsFixture, Fixtures}
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.acls.AclSimpleCheck
import ai.senscience.nexus.delta.sdk.identities.IdentitiesDummy
import ai.senscience.nexus.delta.sdk.marshalling.{RdfExceptionHandler, RdfRejectionHandler}
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.utils.RouteHelpers
import ai.senscience.nexus.delta.sourcing.model.Identity.User
import ai.senscience.nexus.delta.sourcing.model.Label
import ai.senscience.nexus.delta.sourcing.postgres.DoobieScalaTestFixture
import ai.senscience.nexus.testkit.clock.FixedClock
import ai.senscience.nexus.testkit.scalatest.TestMatchers
import ai.senscience.nexus.testkit.scalatest.ce.{CatsEffectSpec, CatsIOValues}
import ai.senscience.nexus.testkit.{CirceEq, CirceLiteral}
import akka.http.scaladsl.server.{ExceptionHandler, RejectionHandler}
import org.scalatest.matchers.should.Matchers
import org.scalatest.{CancelAfterFailure, Inspectors, OptionValues}

trait CompositeViewsRoutesFixtures
    extends CatsEffectSpec
    with RouteHelpers
    with DoobieScalaTestFixture
    with Matchers
    with CatsIOValues
    with CirceLiteral
    with CirceEq
    with FixedClock
    with OptionValues
    with TestMatchers
    with Inspectors
    with CancelAfterFailure
    with CompositeViewsFixture
    with Fixtures {

  implicit val ordering: JsonKeyOrdering =
    JsonKeyOrdering.default(topKeys =
      List("@context", "@id", "@type", "reason", "details", "sourceId", "projectionId", "_total", "_results")
    )

  implicit val baseUri: BaseUri                   = BaseUri.unsafe("http://localhost", "v1")
  implicit val rejectionHandler: RejectionHandler = RdfRejectionHandler.apply
  implicit val exceptionHandler: ExceptionHandler = RdfExceptionHandler.apply

  val realm  = Label.unsafe("myrealm")
  val reader = User("reader", realm)
  val writer = User("writer", realm)

  val identities = IdentitiesDummy.fromUsers(reader, writer)

  val aclCheck = AclSimpleCheck().accepted

}
