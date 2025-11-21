package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.projections.ProjectLastUpdateStream
import ai.senscience.nexus.delta.sourcing.projections.model.ProjectLastUpdate
import cats.effect.{Clock, IO}
import fs2.{Pipe, Stream}
import org.typelevel.otel4s.metrics.Meter

import scala.concurrent.duration.{DurationInt, FiniteDuration}

/**
  * Informs if a given project has been active lately (if a resource has been created or updated in a configured
  * interval)
  */
trait ProjectActivity {

  def apply(project: ProjectRef): IO[Boolean]

}

object ProjectActivity {

  val noop: ProjectActivity = (_: ProjectRef) => IO.pure(false)

  private def evictStream(signals: ProjectActivityMap) =
    Stream.awakeEvery[IO](1.second).evalTap { _ =>
      signals.evictInactiveProjects
    }

  private[stream] def activityPipe(activityMap: ProjectActivityMap): Pipe[IO, ProjectLastUpdate, ProjectLastUpdate] =
    _.groupWithin(100, 100.millis)
      .evalTap { chunk =>
        activityMap.newValues(
          chunk.map { projectLastUpdate =>
            projectLastUpdate.project -> projectLastUpdate.lastInstant
          }.asSeq
        )
      }
      .unchunks
      .concurrently(evictStream(activityMap))

  private val projectionMetadata: ProjectionMetadata =
    ProjectionMetadata("system", "project-activity", None, None)

  def apply(
      supervisor: Supervisor,
      stream: ProjectLastUpdateStream,
      clock: Clock[IO],
      inactiveInterval: FiniteDuration
  )(using Meter[IO]): IO[ProjectActivity] = {
    given ProjectionBackpressure = ProjectionBackpressure.Noop
    for {
      activityMap <- ProjectActivityMap(clock, inactiveInterval)
      compiled     =
        CompiledProjection.fromStream(
          projectionMetadata,
          ExecutionStrategy.EveryNode,
          (offset: Offset) =>
            stream(offset)
              .through(activityPipe(activityMap))
              .drain
        )
      _           <- supervisor.run(compiled)
    } yield apply(activityMap)
  }

  def apply(activityMap: ProjectActivityMap): ProjectActivity =
    (project: ProjectRef) => activityMap.contains(project)
}
