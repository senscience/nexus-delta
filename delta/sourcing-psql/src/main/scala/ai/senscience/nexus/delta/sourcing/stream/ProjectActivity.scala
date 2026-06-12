package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.kernel.search.TimeRange
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.projections.ProjectLastUpdateStream
import ai.senscience.nexus.delta.sourcing.projections.model.ProjectLastUpdate
import cats.effect.{Clock, IO}
import cats.syntax.all.*
import fs2.{Pipe, Stream}
import org.typelevel.otel4s.metrics.Meter

import scala.concurrent.duration.{DurationInt, FiniteDuration}

/**
  * Informs if a given project has been active lately (if a resource has been created or updated in a configured
  * interval) and exposes activation events for downstream consumers.
  */
trait ProjectActivity {

  /**
    * Whether the given project is currently flagged as active.
    */
  def isActive(project: ProjectRef): IO[Boolean]

  /**
    * Stream of projects that have just transitioned from inactive to active. Used by view coordinators to resume
    * passivated projections when activity resumes.
    */
  def activations: Stream[IO, ProjectRef]

  /**
    * Snapshot of the projects currently flagged as active.
    */
  def activeProjects: IO[List[ProjectRef]]

}

object ProjectActivity {

  val noop: ProjectActivity = new ProjectActivity {
    override def isActive(project: ProjectRef): IO[Boolean] = IO.pure(false)
    override def activations: Stream[IO, ProjectRef]        = Stream.empty
    override def activeProjects: IO[List[ProjectRef]]       = IO.pure(List.empty)
  }

  private def refreshStream(signals: ProjectActivityMap, activations: ProjectionActivations) =
    Stream.awakeEvery[IO](1.second).evalTap { _ =>
      signals.refresh.flatMap(publish(activations, _))
    }

  private[stream] def activityPipe(
      activityMap: ProjectActivityMap,
      activations: ProjectionActivations
  ): Pipe[IO, ProjectLastUpdate, ProjectLastUpdate] =
    _.groupWithin(100, 100.millis)
      .evalTap { chunk =>
        activityMap
          .newValues(chunk.map { plu => plu.project -> plu.lastInstant }.asSeq)
          .flatMap(publish(activations, _))
      }
      .unchunks
      .concurrently(refreshStream(activityMap, activations))

  private def publish(activations: ProjectionActivations, projects: Set[ProjectRef]): IO[Unit] =
    projects.toList.traverse_(project => activations.publish(ProjectionActivation.ForProject(project)))

  /**
    * Populates the activity map with the projects that are currently active (updated within the inactivity window),
    * read directly from the store. This makes the active set available at startup, independently of the offset the
    * activity projection resumes from. Does not publish activation events.
    */
  private[stream] def seedActiveProjects(
      stream: ProjectLastUpdateStream,
      activityMap: ProjectActivityMap,
      clock: Clock[IO],
      inactiveInterval: FiniteDuration
  ): IO[Unit] =
    clock.realTimeInstant.flatMap { now =>
      val threshold = now.minusSeconds(inactiveInterval.toSeconds)
      stream
        .projects(TimeRange.After(threshold))
        .compile
        .toList
        .flatMap { projects => activityMap.newValues(projects.map(_ -> now)).void }
    }

  private val projectionMetadata: ProjectionMetadata =
    ProjectionMetadata("system", "project-activity", None, None)

  def apply(
      supervisor: Supervisor,
      stream: ProjectLastUpdateStream,
      clock: Clock[IO],
      inactiveInterval: FiniteDuration,
      projectionActivations: ProjectionActivations
  )(using Meter[IO]): IO[ProjectActivity] =
    for {
      activityMap <- ProjectActivityMap(clock, inactiveInterval)
      _           <- seedActiveProjects(stream, activityMap, clock, inactiveInterval)
      compiled     =
        CompiledProjection.fromStream(
          projectionMetadata,
          ExecutionStrategy.EveryNode,
          (offset: Offset) =>
            stream(offset)
              .through(activityPipe(activityMap, projectionActivations))
              .drain
        )
      _           <- supervisor.run(compiled)
    } yield apply(activityMap, projectionActivations)

  def apply(activityMap: ProjectActivityMap, projectionActivations: ProjectionActivations): ProjectActivity =
    new ProjectActivity {
      override def isActive(project: ProjectRef): IO[Boolean] = activityMap.isActive(project)
      override def activations: Stream[IO, ProjectRef]        =
        projectionActivations.events.collect { case ProjectionActivation.ForProject(project) => project }
      override def activeProjects: IO[List[ProjectRef]]       = activityMap.activeProjects
    }
}
