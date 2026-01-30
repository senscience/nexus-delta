package ai.senscience.nexus.delta.plugins.blazegraph.routes

import ai.senscience.nexus.delta.plugins.blazegraph.indexing.IndexingViewDef.ActiveViewDef
import ai.senscience.nexus.delta.plugins.blazegraph.indexing.{FetchIndexingView, SparqlRestartScheduler}
import ai.senscience.nexus.delta.plugins.blazegraph.model.*
import ai.senscience.nexus.delta.plugins.blazegraph.model.BlazegraphViewRejection.{InvalidResourceId, ViewNotFound}
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress
import ai.senscience.nexus.delta.sdk.directives.ProjectionsDirectives
import ai.senscience.nexus.delta.sdk.model.IdSegment
import ai.senscience.nexus.delta.sdk.model.IdSegment.{IriSegment, StringSegment}
import ai.senscience.nexus.delta.sdk.views.ViewRef
import ai.senscience.nexus.delta.sourcing.model.Identity
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.query.SelectFilter
import cats.effect.{IO, Ref}
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.server.Route

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

  private val runTrigger             = Ref.unsafe[IO, Boolean](false)
  private val sparqlRestartScheduler = new SparqlRestartScheduler {
    override def run(fromOffset: Offset)(using Identity.Subject): IO[Unit] =
      runTrigger.set(true).void
  }

  private lazy val routes =
    Route.seal(
      BlazegraphViewsIndexingRoutes(
        fetchView,
        sparqlRestartScheduler,
        identities,
        aclCheck,
        ProjectionsDirectives.testEcho
      )
    )

  private val viewEndpoint = "/views/myorg/myproject/myid"

  override def beforeAll(): Unit = {
    super.beforeAll()
    val aclOps =
      aclCheck.append(AclAddress.fromProject(projectRef), reader -> Set(permissions.read, permissions.query)) >>
        aclCheck.append(AclAddress.fromProject(projectRef), writer -> Set(permissions.write)) >>
        aclCheck.append(AclAddress.Root, admin -> Set(permissions.write))
    aclOps.accepted
  }

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
    Get(s"$viewEndpoint/statistics") ~> as(reader) ~> routes ~> check {
      response.status shouldEqual StatusCodes.OK
      response.asString shouldEqual "indexing-statistics"
    }
  }

  "fetch offset from view" in {
    Get(s"$viewEndpoint/offset") ~> as(reader) ~> routes ~> check {
      response.status shouldEqual StatusCodes.OK
      response.asString shouldEqual "offset"
    }
  }

  "fail to restart offset from view without resources/write permission" in {
    Delete(s"$viewEndpoint/offset") ~> as(reader) ~> routes ~> check {
      response.shouldBeForbidden
    }
  }

  "restart offset from view" in {
    Delete(s"$viewEndpoint/offset") ~> as(writer) ~> routes ~> check {
      response.status shouldEqual StatusCodes.OK
      response.asString shouldEqual "schedule-restart"
    }
  }

  "return no blazegraph projection failures without write permission" in {
    Get(s"$viewEndpoint/failures") ~> routes ~> check {
      response.shouldBeForbidden
    }
  }

  "return failures as a response" in {
    Get(s"$viewEndpoint/failures") ~> as(writer) ~> routes ~> check {
      response.status shouldBe StatusCodes.OK
      response.asString shouldEqual "indexing-errors"
    }
  }

  "fail to restart full reindexing without write permissions on all projects" in {
    Post("/jobs/sparql/reindex") ~> as(writer) ~> routes ~> check {
      response.shouldBeForbidden
    }
  }

  "restart full reindexing with write permissions on all projects" in {
    Post("/jobs/sparql/reindex") ~> as(admin) ~> routes ~> check {
      response.status shouldBe StatusCodes.Accepted
      runTrigger.get.accepted shouldEqual true
    }
  }

}
