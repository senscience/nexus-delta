package ai.senscience.nexus.delta.plugins.blazegraph.routes

import ai.senscience.nexus.delta.plugins.blazegraph.supervision.SparqlSupervision
import ai.senscience.nexus.delta.plugins.blazegraph.supervision.SparqlSupervision.SparqlNamespaceTriples
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sdk.acls.AclSimpleCheck
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress
import ai.senscience.nexus.delta.sdk.identities.IdentitiesDummy
import ai.senscience.nexus.delta.sdk.permissions.Permissions.supervision
import ai.senscience.nexus.delta.sdk.utils.BaseRouteSpec
import ai.senscience.nexus.delta.sdk.views.ViewRef
import ai.senscience.nexus.delta.sourcing.model.Identity.User
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import cats.effect.IO

class SparqlSupervisionRoutesSpec extends BaseRouteSpec {

  private val supervisor = User("supervisor", realm)

  private val identities = IdentitiesDummy.fromUsers(supervisor)
  private val aclCheck   = AclSimpleCheck(
    (supervisor, AclAddress.Root, Set(supervision.read))
  ).accepted

  private val project = ProjectRef.unsafe("org", "project")
  private val first   = ViewRef(project, nxv + "first")
  private val second  = ViewRef(project, nxv + "second")

  private val blazegraphSupervision = new SparqlSupervision {
    override def get: IO[SparqlSupervision.SparqlNamespaceTriples] = IO.pure(
      SparqlNamespaceTriples(
        153L,
        Map(first -> 42L, second   -> 99L),
        Map("kb"  -> 0L, "unknown" -> 12L)
      )
    )
  }

  private val routes = Route.seal(new BlazegraphSupervisionRoutes(blazegraphSupervision, identities, aclCheck).routes)

  "The blazegraph supervision endpoint" should {
    "be forbidden without supervision/read permission" in {
      Get("/supervision/blazegraph") ~> routes ~> check {
        response.shouldBeForbidden
      }
    }

    "be accessible with supervision/read permission and return expected payload" in {
      val expected =
        json"""
          {
           "total": 153,
           "assigned" : [
             {
               "count" : 42,
               "project" : "org/project",
               "view" : "https://bluebrain.github.io/nexus/vocabulary/first"
             },
             {
               "count" : 99,
               "project" : "org/project",
               "view" : "https://bluebrain.github.io/nexus/vocabulary/second"
             }
           ],
           "unassigned" : [
             {
               "count" : 0,
               "namespace" : "kb"
             },
             {
               "count" : 12,
               "namespace" : "unknown"
             }
           ]
         }"""

      Get("/supervision/blazegraph") ~> as(supervisor) ~> routes ~> check {
        response.status shouldEqual StatusCodes.OK
        response.asJson shouldEqual expected
      }
    }
  }

}
