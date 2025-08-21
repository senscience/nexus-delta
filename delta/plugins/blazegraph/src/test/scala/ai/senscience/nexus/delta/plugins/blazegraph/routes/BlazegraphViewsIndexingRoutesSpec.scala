package ai.senscience.nexus.delta.plugins.blazegraph.routes

import ai.senscience.nexus.delta.plugins.blazegraph.indexing.FetchIndexingView
import ai.senscience.nexus.delta.plugins.blazegraph.indexing.IndexingViewDef.ActiveViewDef
import ai.senscience.nexus.delta.plugins.blazegraph.model.*
import ai.senscience.nexus.delta.plugins.blazegraph.model.BlazegraphViewRejection.{InvalidResourceId, ViewNotFound}
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress
import ai.senscience.nexus.delta.sdk.directives.ProjectionsDirectives
import ai.senscience.nexus.delta.sdk.model.IdSegment
import ai.senscience.nexus.delta.sdk.model.IdSegment.{IriSegment, StringSegment}
import ai.senscience.nexus.delta.sdk.views.ViewRef
import ai.senscience.nexus.delta.sourcing.model.Identity.Anonymous
import ai.senscience.nexus.delta.sourcing.query.SelectFilter
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import cats.effect.IO

class BlazegraphViewsIndexingRoutesSpec extends BlazegraphViewRoutesFixtures {

  private val myId         = nxv + "myid"
  private val indexingView = ActiveViewDef(
    ViewRef(projectRef, myId),
    "projection",
    SelectFilter.latest,
    None,
    "namespace",
    1,
    1
  )

  private def fetchView: FetchIndexingView =
    (id: IdSegment, ref) =>
      id match {
        case IriSegment(`myId`)    => IO.pure(indexingView)
        case IriSegment(id)        => IO.raiseError(ViewNotFound(id, ref))
        case StringSegment("myid") => IO.pure(indexingView)
        case StringSegment(id)     => IO.raiseError(InvalidResourceId(id))
      }

  private lazy val routes =
    Route.seal(
      BlazegraphViewsIndexingRoutes(
        fetchView,
        identities,
        aclCheck,
        ProjectionsDirectives.testEcho
      )
    )

  private val viewEndpoint = "/views/myorg/myproject/myid"

  "fail to fetch statistics and offset from view without resources/read permission" in {
    val endpoints = List(
      s"$viewEndpoint/statistics",
      s"$viewEndpoint/offset"
    )
    forAll(endpoints) { endpoint =>
      Get(endpoint) ~> routes ~> check {
        response.shouldBeForbidden
      }
    }
  }

  "fetch statistics from view" in {
    aclCheck.append(AclAddress.Root, Anonymous -> Set(permissions.read)).accepted

    Get(s"$viewEndpoint/statistics") ~> routes ~> check {
      response.status shouldEqual StatusCodes.OK
      response.asString shouldEqual "indexing-statistics"
    }
  }

  "fetch offset from view" in {
    Get(s"$viewEndpoint/offset") ~> routes ~> check {
      response.status shouldEqual StatusCodes.OK
      response.asString shouldEqual "offset"
    }
  }

  "fail to restart offset from view without resources/write permission" in {
    Delete(s"$viewEndpoint/offset") ~> routes ~> check {
      response.shouldBeForbidden
    }
  }

  "restart offset from view" in {

    aclCheck.append(AclAddress.Root, Anonymous -> Set(permissions.write)).accepted
    Delete(s"$viewEndpoint/offset") ~> routes ~> check {
      response.status shouldEqual StatusCodes.OK
      response.asString shouldEqual "schedule-restart"
    }
  }

  "return no blazegraph projection failures without write permission" in {
    aclCheck.subtract(AclAddress.Root, Anonymous -> Set(permissions.write)).accepted
    Get(s"$viewEndpoint/failures") ~> routes ~> check {
      response.shouldBeForbidden
    }
  }

  "return failures as a response" in {
    aclCheck.append(AclAddress.Root, Anonymous -> Set(permissions.write)).accepted
    Get(s"$viewEndpoint/failures") ~> routes ~> check {
      response.status shouldBe StatusCodes.OK
      response.asString shouldEqual "indexing-errors"
    }
  }

}
