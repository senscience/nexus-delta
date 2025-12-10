package ai.senscience.nexus.delta.sdk.directives

import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.rdf.syntax.jsonOpsSyntax
import ai.senscience.nexus.delta.sdk.utils.BaseRouteSpec
import ai.senscience.nexus.delta.sourcing.model.{EntityType, ProjectRef}
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.postgres.DoobieScalaTestFixture
import ai.senscience.nexus.delta.sourcing.projections.{ProjectionErrors, ProjectionSelector, Projections}
import ai.senscience.nexus.delta.sourcing.query.{EntityTypeFilter, SelectFilter}
import ai.senscience.nexus.delta.sourcing.stream.Elem.FailedElem
import ai.senscience.nexus.delta.sourcing.stream.{ProjectionMetadata, ProjectionProgress}
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.server.Directives.{path, *}
import org.apache.pekko.http.scaladsl.server.Route

import java.time.Instant

class ProjectionsDirectivesSpec extends BaseRouteSpec with DoobieScalaTestFixture {

  private lazy val projections      = Projections(xas, EntityTypeFilter.All, queryConfig, clock)
  private lazy val projectionErrors = ProjectionErrors(xas, queryConfig, clock)

  private lazy val projectionDirectives = ProjectionsDirectives(projections, projectionErrors)

  private val project = ProjectRef.unsafe("myorg", "myproject")
  private val myId    = nxv + "myid"
  private val myId2   = nxv + "myid2"

  private val projectionMetadata = ProjectionMetadata("test", "projection", Some(project), Some(myId))
  private val progress           = ProjectionProgress(Offset.at(15L), Instant.EPOCH, 9000L, 400L, 30L)

  private lazy val routes = Route.seal(
    get {
      concat(
        path("statistics") {
          projectionDirectives.statistics(project, SelectFilter.latest, projectionMetadata.name)
        },
        path("by-name") {
          projectionDirectives.indexingErrors(ProjectionSelector.Name(projectionMetadata.name))
        },
        path("by-project-id") {
          projectionDirectives.indexingErrors(ProjectionSelector.ProjectId(project, myId))
        }
      )
    }
  )

  override def beforeAll(): Unit = {
    super.beforeAll()
    val error = new Exception("boom")
    val rev   = 1
    val fail1 = FailedElem(EntityType("ACL"), myId, project, Instant.EPOCH, Offset.At(42L), error, rev)
    val fail2 = FailedElem(EntityType("Schema"), myId2, project, Instant.EPOCH, Offset.At(43L), error, rev)
    val save  = projections.save(projectionMetadata, progress) >>
      projectionErrors.saveFailedElems(projectionMetadata, List(fail1, fail2))
    save.accepted
  }

  "Fetch statistics for the projection" in {
    Get("/statistics") ~> routes ~> check {
      val expected = json"""
      {
        "@context" : "https://bluebrain.github.io/nexus/contexts/statistics.json",
        "@type" : "ViewStatistics",
        "delayInSeconds" : 0,
        "discardedEvents" : 400,
        "evaluatedEvents" : 8570,
        "failedEvents" : 30,
        "lastEventDateTime" : "1970-01-01T00:00:00Z",
        "lastProcessedEventDateTime" : "1970-01-01T00:00:00Z",
        "processedEvents" : 9000,
        "remainingEvents" : 0,
        "totalEvents" : 9000
      }"""
      response.status shouldEqual StatusCodes.OK
      response.asJson shouldEqual expected
    }
  }

  private val expectedErrors =
    json"""
      {
  "@context": [
    "https://bluebrain.github.io/nexus/contexts/metadata.json",
    "https://bluebrain.github.io/nexus/contexts/search.json",
    "https://bluebrain.github.io/nexus/contexts/error.json"
  ],
  "_total": 2,
  "_results": [
    {
      "id": "https://bluebrain.github.io/nexus/vocabulary/myid",
      "offset": {
        "@type": "At",
        "value": 42
      },
      "_project": "myorg/myproject",
      "_rev": 1,
      "reason": {
        "type": "UnexpectedError",
        "value": {
          "message": "boom",
          "exception" : "java.lang.Exception"
        }
      }
    },
    {
      "id": "https://bluebrain.github.io/nexus/vocabulary/myid2",
      "offset": {
        "@type": "At",
        "value": 43
      },
      "_project": "myorg/myproject",
      "_rev": 1,
      "reason": {
        "type": "UnexpectedError",
        "value": {
          "message": "boom",
          "exception" : "java.lang.Exception"
        }
      }
    }
  ]
}
        """

  "fetch the projection errors by the projection name" in {
    Get("/by-name") ~> routes ~> check {
      response.status shouldEqual StatusCodes.OK
      response.asJson.removeAllKeys("stacktrace") shouldEqual expectedErrors
    }
  }

  "fetch the projection errors by the projection project/id" in {
    Get("/by-project-id") ~> routes ~> check {
      response.status shouldEqual StatusCodes.OK
      response.asJson.removeAllKeys("stacktrace") shouldEqual expectedErrors
    }
  }
}
