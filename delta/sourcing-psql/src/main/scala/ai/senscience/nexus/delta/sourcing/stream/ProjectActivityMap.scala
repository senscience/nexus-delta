package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.stream.ProjectActivityMap.Activity
import cats.effect.std.AtomicCell
import cats.effect.{Clock, IO}
import cats.syntax.all.*
import fs2.concurrent.SignallingRef
import org.typelevel.otel4s.metrics.{Gauge, Meter}

import java.time.Instant
import scala.concurrent.duration.FiniteDuration

/**
  * Keeps track of a list of project signals based on given values
  */
final class ProjectActivityMap private (
    activitiesCell: AtomicCell[IO, Map[ProjectRef, Activity]],
    clock: Clock[IO],
    inactiveInterval: FiniteDuration,
    activeProjectsGauge: Gauge[IO, Long]
) {

  /**
    * Return the signal for the given project
    */
  def signal(project: ProjectRef): IO[Option[SignallingRef[IO, Boolean]]] =
    activitiesCell.get.map(_.get(project).map(_.signal))

  def refresh: IO[Unit] =
    inactivityThreshold.flatMap { inactivityInstant =>
      activitiesCell.evalUpdate { activities =>
        val (active, inactive) = activities.partition { case (_, activity) =>
          activity.updatedAt.isAfter(inactivityInstant)
        }
        inactive.parUnorderedTraverse { _.signal.set(false) } >>
          activeProjectsGauge
            .record(active.size)
            .as(active ++ inactive)
      }
    }

  /**
    * Push updates for values and recompute the signals for all values with the new predicate
    */
  def newValues(updates: Seq[(ProjectRef, Instant)]): IO[Unit] =
    updates.traverse { case (project, lastInstant) =>
      inactivityThreshold.flatMap { inactivityInstant =>
        val isActive = lastInstant.isAfter(inactivityInstant)
        SignallingRef[IO, Boolean](isActive).map(Activity(lastInstant, _)).flatMap { activity =>
          activitiesCell.update(_.updated(project, activity))
        }
      }
    }.void

  private def inactivityThreshold =
    clock.realTimeInstant.map(_.minusSeconds(inactiveInterval.toSeconds))
}

object ProjectActivityMap {

  final private case class Activity(updatedAt: Instant, signal: SignallingRef[IO, Boolean])

  def apply(clock: Clock[IO], inactiveInterval: FiniteDuration)(using Meter[IO]): IO[ProjectActivityMap] =
    for {
      values              <- AtomicCell[IO].of(Map.empty[ProjectRef, Activity])
      activeProjectsGauge <- Meter[IO]
                               .gauge[Long]("nexus.indexing.projects.active.gauge")
                               .withDescription("Gauge of active projects.")
                               .create
    } yield new ProjectActivityMap(values, clock, inactiveInterval, activeProjectsGauge)

}
