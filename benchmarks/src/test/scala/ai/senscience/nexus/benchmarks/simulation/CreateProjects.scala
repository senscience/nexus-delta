package ai.senscience.nexus.benchmarks.simulation

import ai.senscience.nexus.benchmarks.simulation.CreateProjects.BenchmarkConfig
import ai.senscience.nexus.benchmarks.syntax.*
import ai.senscience.nexus.benchmarks.{ConfigLoader, ResourceLoader}
import ai.senscience.nexus.delta.kernel.instances.given
import fs2.io.file.Path
import io.circe.Json
import io.circe.literal.*
import io.gatling.core.Predef.*
import io.gatling.http.Predef.*
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader
import java.util.concurrent.atomic.AtomicInteger

/**
  * Scenario where projects
  */
final class CreateProjects extends BaseSimulation {

  private val config       = ConfigLoader.load[BenchmarkConfig]("benchmarks")
  import config.*
  private val httpProtocol = http.baseUrl(baseUrl)

  private val counter = new AtomicInteger(0)

  private def generateProjectRef = s"$org/${counter.incrementAndGet()}"

  private val crossProjectResolverValue =
    json"""
          {
            "@type": "CrossProject",
            "priority": 0,
            "projects": [
              "benchmarks/datamodels"
            ],
            "useCurrentCaller": true
          }
        """

  private val contextPayloads = ResourceLoader.loadSync(context.prefixPath, context.files)

  private val resourcePayloads = ResourceLoader.loadSync(resources.prefixPath, resources.files)

  private val repeatTimes = resources.repeat.map(_.times).getOrElse(0)

  private val repeatResource = resources.repeat.flatMap { repeat =>
    ResourceLoader.loadSync(resources.prefixPath, repeat.file).hcursor.downField("@id").delete.focus
  }

  private val orgScn = scenario("Creating the root org")
    .exec {
      http("createOrg")
        .put(s"/orgs/$org")
        .jsonBody(Json.obj())
        .check(status.is(201))
    }
    .exec {
      http("createOrg")
        .put("/orgs/benchmarks")
        .jsonBody(Json.obj())
        .check(status.is(201))
    }

  private val datamodelsProjectScn = scenario("Creating the datamodels project")
    .exec {
      http("createDatamodelsProject")
        .put("/projects/benchmarks/datamodels")
        .jsonBody(Json.obj())
        .check(status.is(201))
    }
    .exec(
      contextPayloads.map { c =>
        http("createContext")
          .post("/resources/benchmarks/datamodels/_/")
          .jsonBody(c)
          .check(status.is(201))
      }
    )

  private val projectScn = scenario(s"Populating ${users * times} projects")
    .repeat(times) {
      exec { session =>
        val project = generateProjectRef
        session.set("project", project)
      }.exec {
        http("createProject")
          .put("/projects/#{project}")
          .jsonBody(Json.obj())
          .check(status.is(201))
      }.exec {
        http("createResolver")
          .post("/resolvers/#{project}")
          .jsonBody(crossProjectResolverValue)
          .check(status.is(201))
      }.exec(
        resourcePayloads.map { c =>
          http("createResource")
            .post("/resources/#{project}/_/")
            .jsonBody(c)
            .check(status.is(201))
        }
      ).repeat(repeatTimes) {
        exec(
          repeatResource.map { c =>
            http("createResource")
              .post("/resources/#{project}/_/")
              .jsonBody(c)
              .check(status.is(201))
          }
        )
      }
    }

  private val orgPop        = orgScn.inject(atOnceUsers(1)).protocols(httpProtocol)
  private val dataModelsPop = datamodelsProjectScn.inject(atOnceUsers(1)).protocols(httpProtocol)
  private val projectPop    = projectScn.inject(atOnceUsers(users)).protocols(httpProtocol)

  setUp(
    orgPop
      .andThen(dataModelsPop)
      .andThen(projectPop)
  )
}

object CreateProjects {

  final case class Repeat(file: String, times: Int)

  object Repeat {
    implicit val repeatReader: ConfigReader[Repeat] = deriveReader[Repeat]
  }

  final case class ResourceConfig(prefixPath: Path, files: List[String], repeat: Option[Repeat])

  object ResourceConfig {
    implicit val contextConfigReader: ConfigReader[ResourceConfig] = deriveReader[ResourceConfig]
  }

  final case class BenchmarkConfig(
      baseUrl: String,
      org: String,
      users: Int,
      times: Int,
      context: ResourceConfig,
      resources: ResourceConfig
  )

  object BenchmarkConfig {
    implicit val populateProjectsConfigReaders: ConfigReader[BenchmarkConfig] = deriveReader[BenchmarkConfig]
  }

}
