package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.kernel.search.TimeRange
import ai.senscience.nexus.delta.kernel.utils.CollectionUtils.quote
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

  private val logger = Logger[ProjectActivity]

  val noop: ProjectActivity = new ProjectActivity {
    override def isActive(project: ProjectRef): IO[Boolean] = IO.pure(false)
    override def activations: Stream[IO, ProjectRef]        = Stream.empty
    override def activeProjects: IO[List[ProjectRef]]       = IO.pure(List.empty)
  }

  private def refreshStream(projectActivityMap: ProjectActivityMap) =
    Stream.awakeEvery[IO](1.second).evalTap { _ => projectActivityMap.refresh }

  private[stream] def activityPipe(
      activityMap: ProjectActivityMap,
      activations: ProjectionActivations
  ): Pipe[IO, ProjectLastUpdate, ProjectLastUpdate] =
    _.groupWithin(100, 100.millis)
      .evalTap { chunk =>
        IO.whenA(chunk.nonEmpty)(logger.info(s"New activity for: ${quote(chunk.map(_.project))}")) >>
          activityMap.activeProjects.flatTap { projects => logger.info(s"Active projects: ${quote(projects)}") } >>
          activityMap
            .newValues(chunk.map { plu => plu.project -> plu.lastInstant }.asSeq)
            .flatMap(publish(activations, _))
      }
      .unchunks
      .concurrently(refreshStream(activityMap))

  private def publish(activations: ProjectionActivations, projects: Set[ProjectRef]): IO[Unit] =
    IO.whenA(projects.nonEmpty)(logger.info(s"New active projects: ${quote(projects)}")) >>
      projects.toList.traverse(project => activations.publish(ProjectionActivation.ForProject(project))).void

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
      stream.apply(TimeRange.After(threshold)).compile.toList.flatMap { lastActiveProjects =>
        logger.info(s"${lastActiveProjects.size} projects are active at startup.") >>
          activityMap.newValues(lastActiveProjects.map { plu => plu.project -> plu.lastInstant }).void
      }
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
        projectionActivations.events.debug().collect { case ProjectionActivation.ForProject(project) => project }
      override def activeProjects: IO[List[ProjectRef]]       = activityMap.activeProjects
    }
}
