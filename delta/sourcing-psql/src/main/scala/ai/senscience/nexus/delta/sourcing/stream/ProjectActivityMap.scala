package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.sourcing.model.ProjectRef
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
    activeCell: AtomicCell[IO, Map[ProjectRef, Instant]],
    clock: Clock[IO],
    inactiveInterval: FiniteDuration,
    activeProjectsGauge: Gauge[IO, Long]
) {

  /**
    * Whether the given project is currently flagged as active.
    */
  def isActive(project: ProjectRef): IO[Boolean] =
    activeCell.get.map(_.contains(project))

  /**
    * Snapshot of the projects currently flagged as active.
    */
  def activeProjects: IO[List[ProjectRef]] =
    activeCell.get.map(_.keys.toList)

  /**
    * Drops the projects that have become inactive since the last computation. Always returns an empty set, as a refresh
    * can only age projects out, never bring them back to active.
    */
  def refresh: IO[Set[ProjectRef]] =
    inactivityThreshold.flatMap { threshold =>
      activeCell.evalModify { active =>
        val next = active.filter { case (_, updatedAt) => updatedAt.isAfter(threshold) }
        activeProjectsGauge.record(next.size.toLong).as((next, Set.empty[ProjectRef]))
      }
    }

  /**
    * Pushes updates and recomputes the active set. Returns the projects that just transitioned from inactive (or
    * non-existent) to active.
    */
  def newValues(updates: Seq[(ProjectRef, Instant)]): IO[Set[ProjectRef]] =
    inactivityThreshold.flatMap { threshold =>
      activeCell.modify { active =>
        updates.foldLeft((active, Set.empty[ProjectRef])) { case ((acc, transitioned), (project, lastInstant)) =>
          if lastInstant.isAfter(threshold) then {
            val transition = !acc.contains(project)
            (acc.updated(project, lastInstant), if transition then transitioned + project else transitioned)
          } else (acc - project, transitioned)
        }
      }
    }

  private def inactivityThreshold =
    clock.realTimeInstant.map(_.minusSeconds(inactiveInterval.toSeconds))
}

object ProjectActivityMap {

  def apply(clock: Clock[IO], inactiveInterval: FiniteDuration)(using Meter[IO]): IO[ProjectActivityMap] =
    for {
      values              <- AtomicCell[IO].of(Map.empty[ProjectRef, Instant])
      activeProjectsGauge <- Meter[IO]
                               .gauge[Long]("nexus.indexing.projects.active.gauge")
                               .withDescription("Gauge of active projects.")
                               .create
    } yield new ProjectActivityMap(values, clock, inactiveInterval, activeProjectsGauge)

}
