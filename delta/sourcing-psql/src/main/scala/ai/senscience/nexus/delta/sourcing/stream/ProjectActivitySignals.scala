package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sourcing.model.{EntityType, ProjectRef}
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.projections.ProjectLastUpdateStream
import ai.senscience.nexus.delta.sourcing.projections.model.ProjectLastUpdate
import ai.senscience.nexus.delta.sourcing.stream.Elem.SuccessElem
import cats.effect.{Clock, IO}
import fs2.concurrent.SignallingRef
import fs2.{Pipe, Stream}
import org.typelevel.otel4s.metrics.Meter

import scala.concurrent.duration.{DurationInt, FiniteDuration}

/**
  * Return signals indicating activity signals in the different projects
  */
trait ProjectActivitySignals {

  /**
    * Return the activity signal for the given project
    */
  def apply(project: ProjectRef): IO[Option[SignallingRef[IO, Boolean]]]

}

object ProjectActivitySignals {

  val noop: ProjectActivitySignals = (_: ProjectRef) => IO.none

  private def evictStream(signals: ProjectSignals) =
    Stream.awakeEvery[IO](1.second).evalTap { _ =>
      signals.evictInactiveProjects
    }

  private[stream] def signalPipe(signals: ProjectSignals): Pipe[IO, ProjectLastUpdate, ProjectLastUpdate] =
    _.groupWithin(100, 100.millis)
      .evalTap { chunk =>
        signals.newValues(
          chunk.map { projectLastUpdate =>
            projectLastUpdate.project -> projectLastUpdate.lastInstant
          }.asSeq
        )
      }
      .unchunks
      .concurrently(evictStream(signals))

  // $COVERAGE-OFF$
  private val projectionMetadata: ProjectionMetadata =
    ProjectionMetadata("system", "project-activity-signals", None, None)

  private val entityType: EntityType = EntityType("project-activity-signals")

  private def lastUpdatesId(project: ProjectRef): Iri = nxv + s"projection/project-activity-signals/$project"

  private def successElem(lastUpdate: ProjectLastUpdate): SuccessElem[Unit] =
    SuccessElem(
      entityType,
      lastUpdatesId(lastUpdate.project),
      lastUpdate.project,
      lastUpdate.lastInstant,
      lastUpdate.lastOrdering,
      (),
      1
    )

  def apply(
      supervisor: Supervisor,
      stream: ProjectLastUpdateStream,
      clock: Clock[IO],
      inactiveInterval: FiniteDuration
  )(using Meter[IO]): IO[ProjectActivitySignals] = {

    for {
      signals <- ProjectSignals(clock, inactiveInterval)
      compiled =
        CompiledProjection.fromStream(
          projectionMetadata,
          ExecutionStrategy.EveryNode,
          (offset: Offset) =>
            stream(offset)
              .through(signalPipe(signals))
              .map(successElem)
        )
      _       <- supervisor.run(compiled)
    } yield apply(signals)
  }
  // $COVERAGE-ON$

  def apply(signals: ProjectSignals): ProjectActivitySignals =
    (project: ProjectRef) => signals.getSignal(project)
}
