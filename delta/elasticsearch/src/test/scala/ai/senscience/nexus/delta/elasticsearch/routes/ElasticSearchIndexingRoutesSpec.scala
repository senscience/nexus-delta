package ai.senscience.nexus.delta.elasticsearch.routes

import ai.senscience.nexus.delta.elasticsearch.client.IndexLabel
import ai.senscience.nexus.delta.elasticsearch.indexing.IndexingViewDef.ActiveViewDef
import ai.senscience.nexus.delta.elasticsearch.model.ElasticSearchViewRejection.{InvalidResourceId, ViewNotFound}
import ai.senscience.nexus.delta.elasticsearch.model.permissions as esPermissions
import ai.senscience.nexus.delta.elasticsearch.routes.ElasticSearchIndexingRoutes.FetchIndexingView
import ai.senscience.nexus.delta.elasticsearch.views.DefaultIndexDef
import ai.senscience.nexus.delta.elasticsearch.{ElasticSearchViews, ValidateElasticSearchView}
import ai.senscience.nexus.delta.kernel.utils.UUIDF
import ai.senscience.nexus.delta.rdf.Vocabulary
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress
import ai.senscience.nexus.delta.sdk.model.IdSegment.{IriSegment, StringSegment}
import ai.senscience.nexus.delta.sdk.permissions.Permissions.events
import ai.senscience.nexus.delta.sdk.projects.{FetchContext, FetchContextDummy}
import ai.senscience.nexus.delta.sdk.resolvers.ResolverContextResolution
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sdk.views.{IndexingRev, ViewRef}
import ai.senscience.nexus.delta.sourcing.model.EntityType
import ai.senscience.nexus.delta.sourcing.model.Identity.Anonymous
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.projections.{ProjectionErrors, Projections}
import ai.senscience.nexus.delta.sourcing.query.SelectFilter
import ai.senscience.nexus.delta.sourcing.stream.Elem.FailedElem
import ai.senscience.nexus.delta.sourcing.stream.{PipeChain, ProjectionProgress}
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import cats.effect.IO
import io.circe.JsonObject

import java.time.Instant

class ElasticSearchIndexingRoutesSpec extends ElasticSearchViewsRoutesFixtures {

  implicit private val uuidF: UUIDF = UUIDF.fixed(uuid)

  private lazy val projections      = Projections(xas, None, queryConfig, clock)
  private lazy val projectionErrors = ProjectionErrors(xas, queryConfig, clock)

  implicit private val fetchContext: FetchContext = FetchContextDummy(Map(project.value.ref -> project.value.context))

  private val myId         = nxv + "myid"
  private val myId2        = nxv + "myid2"
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
  private val progress     = ProjectionProgress(Offset.at(15L), Instant.EPOCH, 9000L, 400L, 30L)

  private def fetchView: FetchIndexingView =
    (id, ref) =>
      id match {
        case IriSegment(`myId`)    => IO.pure(indexingView)
        case IriSegment(id)        => IO.raiseError(ViewNotFound(id, ref))
        case StringSegment("myid") => IO.pure(indexingView)
        case StringSegment(id)     => IO.raiseError(InvalidResourceId(id))
      }

  private val allowedPerms = Set(esPermissions.write, esPermissions.read, esPermissions.query, events.read)

  private val defaultIndexDef = DefaultIndexDef(JsonObject(), JsonObject())

  private lazy val views: ElasticSearchViews = ElasticSearchViews(
    fetchContext,
    ResolverContextResolution(rcr),
    ValidateElasticSearchView(
      PipeChain.validate(_, registry),
      IO.pure(allowedPerms),
      (_, _, _) => IO.unit,
      "prefix",
      5,
      xas,
      defaultIndexDef
    ),
    eventLogConfig,
    "prefix",
    xas,
    defaultIndexDef,
    clock
  ).accepted

  private lazy val viewsQuery = new DummyElasticSearchViewsQuery(views)

  private lazy val routes =
    Route.seal(
      ElasticSearchIndexingRoutes(
        identities,
        aclCheck,
        fetchView,
        projections,
        projectionErrors,
        viewsQuery
      )
    )

  override def beforeAll(): Unit = {
    super.beforeAll()
    val error = new Exception("boom")
    val rev   = 1
    val fail1 = FailedElem(EntityType("ACL"), myId, projectRef, Instant.EPOCH, Offset.At(42L), error, rev)
    val fail2 = FailedElem(EntityType("Schema"), myId2, projectRef, Instant.EPOCH, Offset.At(43L), error, rev)
    val save  = for {
      _ <- projections.save(indexingView.projectionMetadata, progress)
      _ <- projectionErrors.saveFailedElems(indexingView.projectionMetadata, List(fail1, fail2))
    } yield ()
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
    aclCheck.append(AclAddress.Root, Anonymous -> Set(esPermissions.read)).accepted

    val expectedResponse =
      json"""
        {
        "@context": "https://bluebrain.github.io/nexus/contexts/statistics.json",
        "@type": "ViewStatistics",
        "delayInSeconds" : 0,
        "discardedEvents": 400,
        "evaluatedEvents": 8570,
        "failedEvents": 30,
        "lastEventDateTime": "${Instant.EPOCH}",
        "lastProcessedEventDateTime": "${Instant.EPOCH}",
        "processedEvents": 9000,
        "remainingEvents": 0,
        "totalEvents": 9000
      }"""

    Get(s"$viewEndpoint/statistics") ~> routes ~> check {
      response.status shouldEqual StatusCodes.OK
      response.asJson shouldEqual expectedResponse
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

  "fail to restart offset from view without views/write permission" in {
    aclCheck.subtract(AclAddress.Root, Anonymous -> Set(esPermissions.write)).accepted

    Delete(s"$viewEndpoint/offset") ~> routes ~> check {
      response.shouldBeForbidden
    }
  }

  "restart offset from view" in {
    aclCheck.append(AclAddress.Root, Anonymous -> Set(esPermissions.write)).accepted
    projections.restarts(Offset.start).compile.toList.accepted.size shouldEqual 0
    Delete(s"$viewEndpoint/offset") ~> routes ~> check {
      response.status shouldEqual StatusCodes.OK
      response.asJson shouldEqual json"""{"@context": "${Vocabulary.contexts.offset}", "@type": "Start"}"""
      projections.restarts(Offset.start).compile.toList.accepted.size shouldEqual 1
    }
  }

  "return no failures without write permission" in {
    aclCheck.subtract(AclAddress.Root, Anonymous -> Set(esPermissions.write)).accepted
    Get(s"$viewEndpoint/failures") ~> routes ~> check {
      response.shouldBeForbidden
    }
  }

  "return failures as a listing" in {
    aclCheck.append(AclAddress.Root, Anonymous -> Set(esPermissions.write)).accepted
    Get(s"$viewEndpoint/failures") ~> routes ~> check {
      response.status shouldBe StatusCodes.OK
      response.asJson.removeAllKeys("stacktrace") shouldEqual jsonContentOf("routes/list-indexing-errors.json")
    }
  }

  "return elasticsearch mapping" in {
    Get(s"$viewEndpoint/_mapping") ~> routes ~> check {
      response.status shouldBe StatusCodes.OK
      response.asJson shouldEqual json"""{"mappings": "mapping"}"""
    }
  }

}
