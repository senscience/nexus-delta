package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.stream.ProjectActivityMap.State
import cats.syntax.order.given
import ai.senscience.nexus.delta.sourcing.implicits.given
import cats.effect.std.AtomicCell
import cats.effect.{Clock, IO}
import org.typelevel.otel4s.metrics.{Gauge, Meter}

import java.time.Instant
import scala.concurrent.duration.FiniteDuration

/**
  * Keeps track of the projects that have been active lately, keyed by the instant of their last update. Only active
  * projects are retained; inactive ones are dropped.
  *
  * State-tracking only — the map does not publish events. Both [[newValues]] and [[refresh]] return the set of projects
  * that just transitioned from inactive (or non-existent) to active. Callers can forward that set to a
  * [[ProjectionActivations]].
  */
final class ProjectActivityMap private (
    stateCell: AtomicCell[IO, State],
    clock: Clock[IO],
    inactiveInterval: FiniteDuration,
    activeProjectsGauge: Gauge[IO, Long]
) {

  /**
    * Whether the given project is currently flagged as active.
    */
  def isActive(project: ProjectRef): IO[Boolean] =
    stateCell.get.map(_.active.contains(project))

  /**
    * Snapshot of the projects currently flagged as active.
    */
  def activeProjects: IO[List[ProjectRef]] =
    stateCell.get.map(_.active.keys.toList)

  /**
    * Drops the projects that have not been updated within the inactivity window, measured against the wall clock.
    */
  def refresh: IO[Unit] =
    inactivityThreshold.flatMap { threshold =>
      stateCell.evalUpdate { state =>
        val next = state.active.filter { case (_, updatedAt) => updatedAt.isAfter(threshold) }
        activeProjectsGauge.record(next.size.toLong).as(state.copy(active = next))
      }
    }

  /**
    * Pushes updates and recomputes the active set. Returns the projects that just transitioned from inactive (or
    * non-existent) to active.
    */
  def newValues(updates: Seq[(ProjectRef, Instant)]): IO[Set[ProjectRef]] =
    stateCell.modify { state =>
      val threshold = state.frontier.minusSeconds(inactiveInterval.toSeconds)
      updates.foldLeft((state, Set.empty[ProjectRef])) { case ((acc, transitioned), (project, lastInstant)) =>
        val transition      = !acc.active.contains(project) && lastInstant.isAfter(threshold)
        val newState        = State(acc.active.updated(project, lastInstant), lastInstant.max(state.frontier))
        val newTransitioned = if transition then transitioned + project else transitioned
        (newState, newTransitioned)
      }
    }

  private def inactivityThreshold: IO[Instant] =
    clock.realTimeInstant.map(_.minusSeconds(inactiveInterval.toSeconds))
}

object ProjectActivityMap {

  /**
    * @param active
    *   the projects currently flagged as active, keyed by the instant of their last update
    * @param frontier
    *   the most recent activity instant ever observed; the activation window is computed relative to it
    */
  final private case class State(active: Map[ProjectRef, Instant], frontier: Instant)

  def apply(clock: Clock[IO], inactiveInterval: FiniteDuration)(using Meter[IO]): IO[ProjectActivityMap] =
    for {
      now                 <- clock.realTimeInstant
      state               <- AtomicCell[IO].of(State(Map.empty, now))
      activeProjectsGauge <- Meter[IO]
                               .gauge[Long]("nexus.indexing.projects.active.gauge")
                               .withDescription("Gauge of active projects.")
                               .create
    } yield new ProjectActivityMap(state, clock, inactiveInterval, activeProjectsGauge)

}
