package ai.senscience.nexus.delta.plugins.blazegraph.routes

import ai.senscience.nexus.delta.plugins.blazegraph.indexing.FetchIndexingView
import ai.senscience.nexus.delta.plugins.blazegraph.indexing.IndexingViewDef.ActiveViewDef
import ai.senscience.nexus.delta.plugins.blazegraph.model.*
import ai.senscience.nexus.delta.plugins.blazegraph.model.BlazegraphViewRejection.{InvalidResourceId, ViewNotFound}
import ai.senscience.nexus.delta.rdf.Vocabulary
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress
import ai.senscience.nexus.delta.sdk.directives.ProjectionsDirectives
import ai.senscience.nexus.delta.sdk.model.IdSegment
import ai.senscience.nexus.delta.sdk.model.IdSegment.{IriSegment, StringSegment}
import ai.senscience.nexus.delta.sdk.views.ViewRef
import ai.senscience.nexus.delta.sourcing.model.Identity.Anonymous
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.projections.Projections
import ai.senscience.nexus.delta.sourcing.query.SelectFilter
import ai.senscience.nexus.delta.sourcing.stream.ProjectionProgress
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import cats.effect.IO

import java.time.Instant

class BlazegraphViewsIndexingRoutesSpec extends BlazegraphViewRoutesFixtures {

  private lazy val projections = Projections(xas, None, queryConfig, clock)

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
  private val progress     = ProjectionProgress(Offset.at(15L), Instant.EPOCH, 9000L, 400L, 30L)

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
        projections,
        ProjectionsDirectives.testEcho
      )
    )

  override def beforeAll(): Unit = {
    super.beforeAll()
    val save = projections.save(indexingView.projectionMetadata, progress)
    save.accepted
  }

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
    val expectedResponse =
      json"""{
        "@context" : "https://bluebrain.github.io/nexus/contexts/offset.json",
        "@type" : "At",
        "value" : 15
      }"""

    Get(s"$viewEndpoint/offset") ~> routes ~> check {
      response.status shouldEqual StatusCodes.OK
      response.asJson shouldEqual expectedResponse
    }
  }

  "fail to restart offset from view without resources/write permission" in {
    Delete(s"$viewEndpoint/offset") ~> routes ~> check {
      response.shouldBeForbidden
    }
  }

  "restart offset from view" in {

    aclCheck.append(AclAddress.Root, Anonymous -> Set(permissions.write)).accepted
    projections.restarts(Offset.start).compile.toList.accepted.size shouldEqual 0
    Delete(s"$viewEndpoint/offset") ~> routes ~> check {
      response.status shouldEqual StatusCodes.OK
      response.asJson shouldEqual json"""{"@context": "${Vocabulary.contexts.offset}", "@type": "Start"}"""
      projections.restarts(Offset.start).compile.toList.accepted.size shouldEqual 1
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
