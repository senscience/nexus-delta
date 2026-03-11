package ai.senscience.nexus.delta.sdk.resources

import ai.senscience.nexus.delta.kernel.search.TimeRange
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sdk.generators.ResourceGen
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.resources.ResourcesExporter.{ExportAlreadyRunning, ProjectStream, ResourceStream}
import ai.senscience.nexus.delta.sdk.utils.Fixtures
import ai.senscience.nexus.delta.sourcing.exporter.ExportConfig.NQuadsExportConfig
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.stream.Elem.SuccessElem
import ai.senscience.nexus.testkit.clock.FixedClock
import ai.senscience.nexus.testkit.file.TempDirectory
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.effect.{Deferred, IO}
import cats.effect.kernel.Resource
import fs2.Stream
import fs2.io.file.{Files, Path}
import munit.catseffect.IOFixture
import org.apache.jena.query.DatasetFactory
import org.apache.jena.riot.{Lang, RDFParser}
import org.http4s.Uri

import java.time.Instant
import scala.jdk.CollectionConverters.*

class ResourcesExporterSuite extends NexusSuite with Fixtures with TempDirectory.Fixture with FixedClock {

  given BaseUri = BaseUri.unsafe("http://localhost", "v1")

  private val project1 = ProjectRef.unsafe("org", "proj1")
  private val project2 = ProjectRef.unsafe("org", "proj2")

  private val id1 = nxv + "1"
  private val id2 = nxv + "2"
  private val id3 = nxv + "3"

  private val resource1 = ResourceGen.sourceToResourceF(
    id1,
    project1,
    jsonContentOf("resources/resource-with-context.json")
  )

  private val resource2 = ResourceGen.sourceToResourceF(
    id2,
    project1,
    jsonContentOf("resources/resource-with-context.json")
  )

  private val resource3 = ResourceGen.sourceToResourceF(
    id1,
    project2,
    jsonContentOf("resources/resource-with-context.json")
  )

  private def mkElem(
      res: ai.senscience.nexus.delta.sdk.DataResource,
      proj: ProjectRef,
      offset: Long,
      instant: Instant
  ) =
    SuccessElem(
      tpe = Resources.entityType,
      id = res.id,
      project = proj,
      instant = instant,
      offset = Offset.at(offset),
      value = res,
      rev = res.rev
    )

  private val resourceStream: ResourceStream = (project, _) => {
    val elems = List(
      mkElem(resource1, project1, 1L, Instant.EPOCH),
      mkElem(resource2, project1, 2L, Instant.EPOCH),
      mkElem(resource3, project2, 3L, Instant.EPOCH)
    ).filter(_.project == project)
    Stream.emits(elems)
  }

  private val projectStream: ProjectStream = _ => Stream.emits(List(project1, project2))

  private val exporter: IOFixture[ResourcesExporter] = ResourceSuiteLocalFixture(
    "exporter",
    Resource.eval(
      IO.defer {
        val config = NQuadsExportConfig(tempDirectory(), Uri.unsafeFromString("http://localhost/v1/resources"))
        ResourcesExporter(resourceStream, projectStream, clock, config)
      }
    )
  )

  override def munitFixtures: Seq[munit.AnyFixture[?]] = List(tempDirectory, exporter)

  private lazy val exportDirectory = tempDirectory()

  test("Export resources for a project as valid N-Quads parseable by Jena") {
    for {
      path    <- exporter().exportProject(project1)
      content <- Files[IO].readUtf8(path).compile.string
    } yield {
      val dataset = DatasetFactory.create()
      RDFParser.create().fromString(content).lang(Lang.NQUADS).parse(dataset)

      val expectedGraph = "http://localhost/v1/resources/org/proj1"
      val graphNames    = dataset.asDatasetGraph().listGraphNodes().asScala.map(_.getURI).toSet
      assertEquals(graphNames, Set(expectedGraph))

      val quads = dataset.asDatasetGraph().find().asScala.toList
      assert(quads.nonEmpty, "Expected non-empty quads")
      quads.foreach { quad =>
        assertEquals(quad.getGraph.getURI, expectedGraph)
      }
    }
  }

  test("Export creates file at the expected path") {
    exporter()
      .exportProject(project1)
      .assertEquals(exportDirectory / "org" / "proj1.nq")
  }

  test("Export resources for a different project produces separate file") {
    for {
      path1    <- exporter().exportProject(project1)
      path2    <- exporter().exportProject(project2)
      content1 <- Files[IO].readUtf8(path1).compile.string
      content2 <- Files[IO].readUtf8(path2).compile.string
    } yield {
      assertNotEquals(path1, path2)
      assert(content1.contains("<http://localhost/v1/resources/org/proj1>"))
      assert(content2.contains("<http://localhost/v1/resources/org/proj2>"))
    }
  }

  test("Export for a project with no resources produces an empty file") {
    val emptyProject = ProjectRef.unsafe("org", "empty")
    for {
      path    <- exporter().exportProject(emptyProject)
      content <- Files[IO].readUtf8(path).compile.string
    } yield {
      assert(content.isEmpty, s"Expected empty file but got: $content")
    }
  }

  test("Export excludes resources created after the export started") {
    val beforeExport = Instant.EPOCH
    val afterExport  = Instant.EPOCH.plusSeconds(60)

    val resource3 = ResourceGen.sourceToResourceF(
      id3,
      project1,
      jsonContentOf("resources/resource-with-context.json")
    )

    val streamWithFutureResource: ResourceStream = (project, _) => {
      val elems = List(
        mkElem(resource1, project1, 1L, beforeExport),
        mkElem(resource2, project1, 2L, beforeExport),
        mkElem(resource3, project1, 3L, afterExport)
      ).filter(_.project == project)
      Stream.emits(elems)
    }

    val config = NQuadsExportConfig(exportDirectory, Uri.unsafeFromString("http://localhost/v1/resources"))

    // Clock returns EPOCH, so resources at afterExport should be excluded
    for {
      cutoffExporter <- ResourcesExporter(streamWithFutureResource, projectStream, clock, config)
      path           <- cutoffExporter.exportProject(project1)
      content        <- Files[IO].readUtf8(path).compile.string
    } yield {
      val dataset = DatasetFactory.create()
      RDFParser.create().fromString(content).lang(Lang.NQUADS).parse(dataset)

      val quads = dataset.asDatasetGraph().find().asScala.toList
      assert(quads.nonEmpty, "Expected non-empty quads")

      // resource3 (id3) should not be present
      val subjects = quads.map(_.getSubject.getURI).toSet
      assert(
        !subjects.contains(id3.toString),
        s"Resource created after export start should be excluded, got: $subjects"
      )
      assert(subjects.contains(id1.toString), "Resource created before export start should be included")
    }
  }

  test("Export all projects in parallel") {
    val expected = Set(exportDirectory / "org" / "proj1.nq", exportDirectory / "org" / "proj2.nq")
    exporter()
      .exportAll()
      .map(_.toSet)
      .assertEquals(expected)
  }

  test("Export only projects updated after a given date") {
    val cutoff                        = Instant.parse("2024-06-01T00:00:00Z")
    val afterRange                    = TimeRange.After(cutoff)
    val filteredStream: ProjectStream = {
      case `afterRange` => Stream.emit(project1)
      case _            => Stream.emits(List(project1, project2))
    }
    val config                        = NQuadsExportConfig(exportDirectory, Uri.unsafeFromString("http://localhost/v1/resources"))
    for {
      exp   <- ResourcesExporter(resourceStream, filteredStream, clock, config)
      paths <- exp.exportAll(afterRange)
    } yield {
      assertEquals(paths.map(_.fileName.toString), List("proj1.nq"))
    }
  }

  test("Reject concurrent export for the same project") {
    val config = NQuadsExportConfig(exportDirectory, Uri.unsafeFromString("http://localhost/v1/resources"))
    for {
      gate     <- Deferred[IO, Unit]
      exporter <- ResourcesExporter(
                    (project, offset) => fs2.Stream.eval(gate.get) >> resourceStream(project, offset),
                    projectStream,
                    clock,
                    config
                  )
      fiber    <- exporter.exportProject(project1).start
      _        <- IO.cede
      result   <- exporter.exportProject(project1).attempt
      _        <- gate.complete(())
      _        <- fiber.joinWithNever
    } yield {
      result match {
        case Left(ExportAlreadyRunning(p)) => assertEquals(p, project1)
        case other                         => fail(s"Expected ExportAlreadyRunning but got $other")
      }
    }
  }
}
