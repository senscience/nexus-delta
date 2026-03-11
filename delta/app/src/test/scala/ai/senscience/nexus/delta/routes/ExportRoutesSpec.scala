package ai.senscience.nexus.delta.routes

import ai.senscience.nexus.delta.kernel.search.TimeRange
import ai.senscience.nexus.delta.sdk.acls.AclSimpleCheck
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress.Root
import ai.senscience.nexus.delta.sdk.identities.IdentitiesDummy
import ai.senscience.nexus.delta.sdk.permissions.Permissions
import ai.senscience.nexus.delta.sdk.resources.ResourcesExporter
import ai.senscience.nexus.delta.sdk.utils.BaseRouteSpec
import ai.senscience.nexus.delta.sourcing.exporter.Exporter.ExportResult
import ai.senscience.nexus.delta.sourcing.exporter.{ExportEventQuery, Exporter}
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import cats.effect.{IO, Ref}
import fs2.io.file.Path
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.server.Route

import java.time.Instant

class ExportRoutesSpec extends BaseRouteSpec {

  private val identities = IdentitiesDummy.fromUsers(alice)

  private val exportTrigger         = Ref.unsafe[IO, Boolean](false)
  private val resourceExportTrigger = Ref.unsafe[IO, Option[ProjectRef]](None)
  private val exportAllTrigger      = Ref.unsafe[IO, Option[TimeRange]](None)

  private val aclCheck = AclSimpleCheck((alice, Root, Set(Permissions.exporter.run))).accepted

  private val exporter = new Exporter {
    override def events(query: ExportEventQuery): IO[ExportResult] =
      exportTrigger.set(true).as(ExportResult(Path("target"), Path("success")))
  }

  private val resourcesExporter = new ResourcesExporter {
    override def exportProject(project: ProjectRef): IO[Path] =
      resourceExportTrigger.set(Some(project)).as(Path(s"target/${project.project}.nq"))

    override def exportAll(timeRange: TimeRange): IO[List[Path]] =
      exportAllTrigger.set(Some(timeRange)).as(List(Path("target/proj1.nq"), Path("target/proj2.nq")))
  }

  private lazy val routes = Route.seal(
    new ExportRoutes(
      identities,
      aclCheck,
      exporter,
      resourcesExporter
    ).routes
  )

  "The export route" should {
    val query =
      json"""{ "output": "export-test", "projects": ["org/proj", "org/proj2"], "offset": {"@type": "At", "value": 2}  }"""
    "fail triggering the export the 'export/run' permission" in {
      Post("/v1/export/events", query.toEntity) ~> routes ~> check {
        response.shouldBeForbidden
        exportTrigger.get.accepted shouldEqual false
      }
    }

    "trigger the 'export/run' permission" in {
      Post("/v1/export/events", query.toEntity) ~> as(alice) ~> routes ~> check {
        response.status shouldEqual StatusCodes.Accepted
        exportTrigger.get.accepted shouldEqual true
      }
    }
  }

  "The resource export route" should {
    "fail without the 'export/run' permission" in {
      Post("/v1/export/resources/org/proj") ~> routes ~> check {
        response.shouldBeForbidden
        resourceExportTrigger.get.accepted shouldEqual None
      }
    }

    "trigger a resource export for the given project" in {
      Post("/v1/export/resources/org/proj") ~> as(alice) ~> routes ~> check {
        response.status shouldEqual StatusCodes.Accepted
        resourceExportTrigger.get.accepted shouldEqual Some(ProjectRef.unsafe("org", "proj"))
      }
    }

    "fail exporting all resources without the 'export/run' permission" in {
      Post("/v1/export/resources") ~> routes ~> check {
        response.shouldBeForbidden
        exportAllTrigger.get.accepted shouldEqual None
      }
    }

    "trigger an export of all projects" in {
      Post("/v1/export/resources") ~> as(alice) ~> routes ~> check {
        response.status shouldEqual StatusCodes.Accepted
        exportAllTrigger.get.accepted shouldEqual Some(TimeRange.Anytime)
      }
    }

    "trigger an export of projects updated within a time range" in {
      exportAllTrigger.set(None).accepted
      Post("/v1/export/resources?updatedAt=2024-06-01T00:00:00Z..*") ~> as(alice) ~> routes ~> check {
        response.status shouldEqual StatusCodes.Accepted
        exportAllTrigger.get.accepted shouldEqual Some(TimeRange.After(Instant.parse("2024-06-01T00:00:00Z")))
      }
    }
  }

}
