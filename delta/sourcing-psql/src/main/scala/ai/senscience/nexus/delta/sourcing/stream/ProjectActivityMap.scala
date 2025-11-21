package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import cats.effect.std.AtomicCell
import cats.effect.{Clock, IO}
import cats.syntax.all.*
import org.typelevel.otel4s.metrics.{Gauge, Meter}

import java.time.Instant
import scala.concurrent.duration.FiniteDuration

/**
  * Keeps track of a list of project signals based on given values
  */
final class ProjectActivityMap(
    activeProjects: AtomicCell[IO, Map[ProjectRef, Instant]],
    clock: Clock[IO],
    inactiveInterval: FiniteDuration,
    activeProjectsGauge: Gauge[IO, Long]
) {

  /**
    * Return the signal for the given project
    */
  def contains(project: ProjectRef): IO[Boolean] =
    activeProjects.get.map(_.contains(project))

  def evictInactiveProjects: IO[Unit] =
    inactivityThreshold.flatMap { inactivityInstant =>
      activeProjects.evalUpdate { projects =>
        val active = projects.filter { case (_, lastUpdate) =>
          lastUpdate.isAfter(inactivityInstant)
        }
        activeProjectsGauge
          .record(active.size)
          .as(active)
      }
    }

  /**
    * Push updates for values and recompute the signals for all values with the new predicate
    */
  def newValues(updates: Seq[(ProjectRef, Instant)]): IO[Unit] =
    updates.traverse { case (project, lastInstant) =>
      inactivityThreshold.flatMap { inactivityInstant =>
        val isActive = lastInstant.isAfter(inactivityInstant)
        IO.whenA(isActive) {
          activeProjects.update(_.updated(project, lastInstant))
        }
      }
    }.void

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
