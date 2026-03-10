package ai.senscience.nexus.delta.sdk.resources

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.rdf.graph.NTriples
import ai.senscience.nexus.delta.rdf.jsonld.api.{JsonLdApi, TitaniumJsonLdApi}
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.sdk.DataResource
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sourcing.exporter.ExportConfig.NQuadsExportConfig
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.stream.SuccessElemStream
import cats.effect.{Clock, IO, Ref, Resource}
import fs2.io.file.{Files, Path}

import java.time.Instant

/**
  * Exports resources from scoped_states as N-Quads files, one file per project.
  *
  * Each resource's expanded JSON-LD is converted to N-Triples, and the project graph URI is appended to produce
  * N-Quads.
  */
trait ResourcesExporter {

  /**
    * Export resources for the given project as N-Quads. Only one export per project can run at a time.
    *
    * @param project
    *   the project to export
    * @return
    *   the path to the exported file
    */
  def exportProject(project: ProjectRef): IO[Path]
}

object ResourcesExporter {

  private val logger = Logger[ResourcesExporter]

  /** Error returned when an export for the given project is already running. */
  final case class ExportAlreadyRunning(project: ProjectRef)
      extends Exception(s"An export for project '$project' is already running.")

  type ResourceStream = (ProjectRef, Offset) => SuccessElemStream[DataResource]

  given JsonLdApi = TitaniumJsonLdApi.lenient

  def apply(resources: Resources, clock: Clock[IO], config: NQuadsExportConfig)(using
      BaseUri,
      RemoteContextResolution
  ): IO[ResourcesExporter] =
    apply(
      resources.currentStates(_, _).map(_.mapValue(_.toResource)),
      clock,
      config
    )

  def apply(resourceStream: ResourceStream, clock: Clock[IO], config: NQuadsExportConfig)(using
      BaseUri,
      RemoteContextResolution
  ): IO[ResourcesExporter] =
    Ref.of[IO, Set[ProjectRef]](Set.empty).map { running =>
      new ResourcesExporter {

        private def acquire(project: ProjectRef): IO[Unit] =
          running
            .modify { set =>
              if set.contains(project) then (set, false)
              else (set + project, true)
            }
            .flatMap {
              case true  => IO.unit
              case false => IO.raiseError(ExportAlreadyRunning(project))
            }

        private def release(project: ProjectRef): IO[Unit] =
          running.update(_ - project)

        override def exportProject(project: ProjectRef): IO[Path] =
          Resource.make(acquire(project))(_ => release(project)).use(_ => doExport(project))

        private def doExport(project: ProjectRef): IO[Path] = {
          val outputFile = config.target / s"${project.organization}" / s"${project.project}.nq"
          for {
            startedAt <- clock.realTimeInstant
            _         <- logger.info(s"Starting resource export for project '$project'")
            _         <- Files[IO].createDirectories(outputFile.parent.get)
            _         <- streamNQuads(project, startedAt)
                           .through(fs2.text.utf8.encode)
                           .through(Files[IO].writeAll(outputFile))
                           .compile
                           .drain
            _         <- logger.info(s"Completed resource export for project '$project' to '$outputFile'")
          } yield outputFile
        }

        private def streamNQuads(project: ProjectRef, startedAt: Instant): fs2.Stream[IO, String] = {
          val graphUri = s"<${config.graphBaseUri}/${project.organization}/${project.project}>"
          resourceStream(project, Offset.start)
            .takeWhile(elem => !elem.instant.isAfter(startedAt))
            .evalMap { elem =>
              val resourceF = elem.value
              resourceF.toNTriples.map(toNQuads(_, graphUri))
            }
        }

        private def toNQuads(ntriples: NTriples, graphUri: String): String =
          ntriples.value
            .split("\n")
            .filter(_.trim.nonEmpty)
            .map { line =>
              val trimmed = line.stripTrailing()
              if trimmed.endsWith(" .") then s"${trimmed.dropRight(2)} $graphUri ."
              else s"$trimmed $graphUri ."
            }
            .mkString("\n") + "\n"
      }
    }
}
