package ai.senscience.nexus.delta.plugins.projectdeletion

import ai.senscience.nexus.delta.projectdeletion.ProjectDeletionRoutes
import ai.senscience.nexus.delta.projectdeletion.model.{contexts, ProjectDeletionConfig}
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.utils.RouteHelpers
import ai.senscience.nexus.pekko.marshalling.RdfMediaTypes
import ai.senscience.nexus.testkit.scalatest.ce.CatsEffectSpec
import org.apache.pekko.http.scaladsl.model.StatusCodes

import scala.concurrent.duration.DurationInt

class ProjectDeletionRoutesSpec extends CatsEffectSpec with RouteHelpers {

  private given JsonKeyOrdering         = JsonKeyOrdering.default()
  private given BaseUri                 = BaseUri.unsafe("http://localhost", "v1")
  private given RemoteContextResolution = RemoteContextResolution.loadResourcesUnsafe(contexts.definition)

  "A ProjectDeletionRoutes" should {
    val config     = ProjectDeletionConfig(
      idleInterval = 10.minutes,
      idleCheckPeriod = 5.seconds,
      deleteDeprecatedProjects = true,
      includedProjects = List("some.+".r),
      excludedProjects = List(".+".r)
    )
    val routes     = new ProjectDeletionRoutes(config)
    val configJson = jsonContentOf("project-deletion-config.json")
    "return the project deletion configuration" in {
      Get("/v1/project-deletion/config") ~> routes.routes ~> check {
        status shouldEqual StatusCodes.OK
        contentType.mediaType shouldEqual RdfMediaTypes.`application/ld+json`
        response.asJson shouldEqual configJson
      }
    }
  }

}
