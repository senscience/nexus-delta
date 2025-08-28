package ai.senscience.nexus.delta.elasticsearch.routes

import ai.senscience.nexus.delta.elasticsearch.client.IndexLabel
import ai.senscience.nexus.delta.elasticsearch.indexing.IndexingViewDef.ActiveViewDef
import ai.senscience.nexus.delta.elasticsearch.indexing.{ElasticsearchRestartScheduler, FetchIndexingView}
import ai.senscience.nexus.delta.elasticsearch.model.ElasticSearchViewRejection.{InvalidResourceId, ViewNotFound}
import ai.senscience.nexus.delta.elasticsearch.model.permissions as esPermissions
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress
import ai.senscience.nexus.delta.sdk.directives.ProjectionsDirectives
import ai.senscience.nexus.delta.sdk.model.IdSegment.{IriSegment, StringSegment}
import ai.senscience.nexus.delta.sdk.views.{IndexingRev, ViewRef}
import ai.senscience.nexus.delta.sourcing.model.Identity
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.query.SelectFilter
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import cats.effect.{IO, Ref}
import io.circe.JsonObject

class ElasticSearchIndexingRoutesSpec extends ElasticSearchViewsRoutesFixtures {

  private val myId         = nxv + "myid"
  private val indexingView = ActiveViewDef(
    ViewRef(projectRef, myId),
    "projection",
    None,
    SelectFilter.latest,
    IndexLabel.unsafe("index"),
    JsonObject.empty,
    JsonObject.empty,
    None,
    IndexingRev.init,
    1
  )

  private def fetchView: FetchIndexingView =
    (id, ref) =>
      id match {
        case IriSegment(`myId`)    => IO.pure(indexingView)
        case IriSegment(id)        => IO.raiseError(ViewNotFound(id, ref))
        case StringSegment("myid") => IO.pure(indexingView)
        case StringSegment(id)     => IO.raiseError(InvalidResourceId(id))
      }

  private val runTrigger         = Ref.unsafe[IO, Boolean](false)
  private val esRestartScheduler = new ElasticsearchRestartScheduler {
    override def run(fromOffset: Offset)(implicit subject: Identity.Subject): IO[Unit] =
      runTrigger.set(true).void
  }

  private val esMapping   = json"""{"mappings": "mapping"}"""
  private lazy val routes =
    Route.seal(
      new ElasticSearchIndexingRoutes(
        identities,
        aclCheck,
        fetchView,
        esRestartScheduler,
        ProjectionsDirectives.testEcho,
        (_: ActiveViewDef) => IO.pure(esMapping)
      ).routes
    )

  private val viewEndpoint = "/views/myorg/myproject/myid"

  override def beforeAll(): Unit = {
    super.beforeAll()
    val aclOps =
      aclCheck.append(AclAddress.fromProject(projectRef), reader -> Set(esPermissions.read)) >>
        aclCheck.append(AclAddress.fromProject(projectRef), writer -> Set(esPermissions.write)) >>
        aclCheck.append(AclAddress.Root, admin -> Set(esPermissions.write))
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

  "fail to restart offset from view without views/write permission" in {
    Delete(s"$viewEndpoint/offset") ~> routes ~> check {
      response.shouldBeForbidden
    }
  }

  "restart offset from view" in {
    Delete(s"$viewEndpoint/offset") ~> as(writer) ~> routes ~> check {
      response.status shouldEqual StatusCodes.OK
      response.asString shouldEqual "schedule-restart"
    }
  }

  "return no failures without write permission" in {
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

  "return elasticsearch mapping" in {
    Get(s"$viewEndpoint/_mapping") ~> as(writer) ~> routes ~> check {
      response.status shouldBe StatusCodes.OK
      response.asJson shouldEqual esMapping
    }
  }

  "fail to restart full reindexing without write permissions on all projects" in {
    Post("/jobs/elasticsearch/reindex") ~> as(writer) ~> routes ~> check {
      response.shouldBeForbidden
    }
  }

  "restart full reindexing with write permissions on all projects" in {
    Post("/jobs/elasticsearch/reindex") ~> as(admin) ~> routes ~> check {
      response.status shouldBe StatusCodes.Accepted
      runTrigger.get.accepted shouldEqual true
    }
  }

}
