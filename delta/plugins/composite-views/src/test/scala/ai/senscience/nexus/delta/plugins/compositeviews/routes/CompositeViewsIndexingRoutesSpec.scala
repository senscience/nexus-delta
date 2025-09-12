package ai.senscience.nexus.delta.plugins.compositeviews.routes

import ai.senscience.nexus.delta.kernel.utils.UrlUtils.encodeUriPath
import ai.senscience.nexus.delta.plugins.compositeviews.CompositeViewsGen
import ai.senscience.nexus.delta.plugins.compositeviews.indexing.CompositeViewDef.ActiveViewDef
import ai.senscience.nexus.delta.plugins.compositeviews.model.CompositeRestart.{FullRebuild, FullRestart, PartialRebuild}
import ai.senscience.nexus.delta.plugins.compositeviews.model.permissions
import ai.senscience.nexus.delta.plugins.compositeviews.projections.{CompositeIndexingDetails, CompositeProjections}
import ai.senscience.nexus.delta.plugins.compositeviews.store.CompositeRestartStore
import ai.senscience.nexus.delta.plugins.compositeviews.stream.CompositeBranch.Run.Main
import ai.senscience.nexus.delta.plugins.compositeviews.stream.{CompositeBranch, CompositeProgress}
import ai.senscience.nexus.delta.plugins.compositeviews.test.{expandOnlyIris, expectIndexingView}
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress
import ai.senscience.nexus.delta.sdk.directives.ProjectionsDirectives
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sdk.views.ViewRef
import ai.senscience.nexus.delta.sourcing.config.QueryConfig
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.query.RefreshStrategy
import ai.senscience.nexus.delta.sourcing.stream.config.BatchConfig
import ai.senscience.nexus.delta.sourcing.stream.{ProjectionProgress, RemainingElems}
import cats.effect.IO
import io.circe.Json
import io.circe.syntax.*
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.server.Route

import java.time.Instant
import scala.concurrent.duration.*
class CompositeViewsIndexingRoutesSpec extends CompositeViewsRoutesFixtures {

  private val now      = Instant.now()
  private val nowPlus5 = now.plusSeconds(5)

  private val myId         = nxv + "myid"
  private val view         = CompositeViewsGen.resourceFor(projectRef, myId, uuid, viewValue, source = Json.obj())
  private val indexingView = ActiveViewDef(
    ViewRef(view.value.project, view.id),
    view.value.uuid,
    view.rev,
    viewValue
  )

  private lazy val restartStore = new CompositeRestartStore(xas)
  private lazy val projections  =
    CompositeProjections(
      restartStore,
      xas,
      QueryConfig(5, RefreshStrategy.Stop),
      BatchConfig(5, 100.millis),
      3.seconds,
      clock
    )

  private def lastRestart = restartStore.last(ViewRef(project.ref, myId)).map(_.flatMap(_.toOption)).accepted

  private val details: CompositeIndexingDetails = new CompositeIndexingDetails(
    _ =>
      IO.pure(
        CompositeProgress(
          Map(
            CompositeBranch(projectSource.id, esProjection.id, Main)         ->
              ProjectionProgress(Offset.at(3L), now, 6, 1, 1),
            CompositeBranch(projectSource.id, blazegraphProjection.id, Main) ->
              ProjectionProgress(Offset.at(3L), now, 6, 1, 1)
          )
        )
      ),
    (_, _, _) => IO.pure(Some(RemainingElems(10, nowPlus5))),
    "prefix"
  )

  private lazy val routes =
    Route.seal(
      CompositeViewsIndexingRoutes(
        identities,
        aclCheck,
        expectIndexingView(indexingView, "myid"),
        expandOnlyIris,
        details,
        projections,
        ProjectionsDirectives.testEcho
      )
    )

  override def beforeAll(): Unit = {
    super.beforeAll()
    aclCheck.append(AclAddress.Root, reader -> Set(permissions.read)).accepted
    aclCheck.append(AclAddress.Root, writer -> Set(permissions.write)).accepted
  }

  private val bgProjectionEncodedId = encodeUriPath(blazegraphProjection.id.toString)

  private val viewEndpoint = "/views/myorg/myproj/myid"

  "Composite views routes" should {

    "fail to fetch/delete offset without permission" in {
      val endpoints = List(
        s"$viewEndpoint/offset",
        s"$viewEndpoint/projections/_/offset",
        s"$viewEndpoint/projections/$bgProjectionEncodedId/offset"
      )
      forAll(endpoints) { endpoint =>
        forAll(List(Get(endpoint), Delete(endpoint))) { req =>
          req ~> routes ~> check {
            response.shouldBeForbidden
          }
        }
      }

      lastRestart shouldEqual None
    }

    "fetch offsets" in {
      val viewOffsets       = jsonContentOf("routes/responses/view-offsets.json")
      val projectionOffsets = jsonContentOf("routes/responses/view-offsets-projection.json")
      val endpoints         = List(
        s"$viewEndpoint/offset"                                    -> viewOffsets,
        s"$viewEndpoint/projections/_/offset"                      -> viewOffsets,
        s"$viewEndpoint/projections/$bgProjectionEncodedId/offset" -> projectionOffsets
      )
      forAll(endpoints) { case (endpoint, expected) =>
        Get(endpoint) ~> as(reader) ~> routes ~> check {
          response.status shouldEqual StatusCodes.OK
          response.asJson shouldEqual expected
        }
      }
    }

    "fetch statistics" in {
      val encodedSource   = encodeUriPath(projectSource.id.toString)
      val viewStats       = jsonContentOf(
        "routes/responses/view-statistics.json",
        "last"                  -> nowPlus5,
        "instant_elasticsearch" -> now,
        "instant_blazegraph"    -> now
      )
      val projectionStats = jsonContentOf(
        "routes/responses/view-statistics-projection.json",
        "last"    -> nowPlus5,
        "instant" -> now
      )
      val sourceStats     = jsonContentOf(
        "routes/responses/view-statistics-source.json",
        "last"                  -> nowPlus5,
        "instant_elasticsearch" -> now,
        "instant_blazegraph"    -> now
      )
      val endpoints       = List(
        s"$viewEndpoint/statistics"                                    -> viewStats,
        s"$viewEndpoint/projections/_/statistics"                      -> viewStats,
        s"$viewEndpoint/projections/$bgProjectionEncodedId/statistics" -> projectionStats,
        s"$viewEndpoint/sources/$encodedSource/statistics"             -> sourceStats
      )
      forAll(endpoints) { case (endpoint, expected) =>
        Get(endpoint) ~> as(reader) ~> routes ~> check {
          response.status shouldEqual StatusCodes.OK
          response.asJson shouldEqual expected
        }
      }
    }

    "fail to fetch indexing description without permission" in {
      Get(s"$viewEndpoint/description") ~> routes ~> check {
        response.shouldBeForbidden
      }
    }

    "fetch indexing description" in {
      Get(s"$viewEndpoint/description") ~> as(reader) ~> routes ~> check {
        response.status shouldEqual StatusCodes.OK
        response.asJson shouldEqual jsonContentOf(
          "routes/responses/view-indexing-description.json",
          "uuid"                  -> uuid,
          "last"                  -> nowPlus5,
          "instant_elasticsearch" -> now,
          "instant_blazegraph"    -> now
        )
      }
    }

    "delete offsets" in {
      val viewOffsets       =
        jsonContentOf("routes/responses/view-offsets.json").replaceKeyWithValue("offset", Offset.start.asJson)
      val projectionOffsets =
        jsonContentOf("routes/responses/view-offsets-projection.json").replaceKeyWithValue(
          "offset",
          Offset.start.asJson
        )

      Delete(s"$viewEndpoint/offset") ~> as(writer) ~> routes ~> check {
        response.status shouldEqual StatusCodes.OK
        response.asJson shouldEqual viewOffsets
        lastRestart.value shouldEqual FullRestart(indexingView.ref, Instant.EPOCH, writer)
      }

      val endpoints = List(
        (
          s"$viewEndpoint/projections/_/offset",
          viewOffsets,
          FullRebuild(indexingView.ref, Instant.EPOCH, writer)
        ),
        (
          s"$viewEndpoint/projections/$bgProjectionEncodedId/offset",
          projectionOffsets,
          PartialRebuild(indexingView.ref, blazegraphProjection.id, Instant.EPOCH, writer)
        )
      )
      forAll(endpoints) { case (endpoint, expectedResult, restart) =>
        Delete(endpoint) ~> as(writer) ~> routes ~> check {
          response.status shouldEqual StatusCodes.OK
          response.asJson shouldEqual expectedResult
          lastRestart.value shouldEqual restart
        }
      }
    }

    "return no failures without write permission" in {
      Get(s"$viewEndpoint/failures") ~> as(reader) ~> routes ~> check {
        response.shouldBeForbidden
      }
    }

    "return failures as a listing" in {
      Get(s"$viewEndpoint/failures") ~> as(writer) ~> routes ~> check {
        response.status shouldBe StatusCodes.OK
        response.asString shouldEqual "indexing-errors"
      }
    }
  }
}
