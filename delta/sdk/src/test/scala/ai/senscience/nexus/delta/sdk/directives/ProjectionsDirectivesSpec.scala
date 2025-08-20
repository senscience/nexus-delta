package ai.senscience.nexus.delta.sdk.directives

import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.rdf.syntax.jsonOpsSyntax
import ai.senscience.nexus.delta.sdk.utils.BaseRouteSpec
import ai.senscience.nexus.delta.sourcing.model.{EntityType, ProjectRef}
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.projections.{ProjectionErrors, ProjectionSelector}
import ai.senscience.nexus.delta.sourcing.stream.Elem.FailedElem
import ai.senscience.nexus.delta.sourcing.stream.ProjectionMetadata
import akka.http.scaladsl.model.*
import akka.http.scaladsl.server.Directives.{path, *}
import akka.http.scaladsl.server.Route

import java.time.Instant

class ProjectionsDirectivesSpec extends BaseRouteSpec {

  private lazy val projectionErrors = ProjectionErrors(xas, queryConfig, clock)

  private lazy val projectionDirectives = ProjectionsDirectives(projectionErrors)

  private val project = ProjectRef.unsafe("myorg", "myproject")
  private val myId    = nxv + "myid"
  private val myId2   = nxv + "myid2"

  private val projectionMetadata = ProjectionMetadata("test", "projection", Some(project), Some(myId))

  private lazy val routes = Route.seal(
    get {
      concat(
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
    val save  = projectionErrors.saveFailedElems(projectionMetadata, List(fail1, fail2))
    save.accepted
  }

  private val expectedResponse =
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
      response.asJson.removeAllKeys("stacktrace") shouldEqual expectedResponse
    }
  }

  "fetch the projection errors by the projection project/id" in {
    Get("/by-project-id") ~> routes ~> check {
      response.status shouldEqual StatusCodes.OK
      response.asJson.removeAllKeys("stacktrace") shouldEqual expectedResponse
    }
  }
}
