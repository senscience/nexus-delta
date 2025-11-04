package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.sourcing.model.ProjectRef
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
final class ProjectSignals(
    activeProjects: AtomicCell[IO, Map[ProjectRef, Instant]],
    signals: AtomicCell[IO, Map[ProjectRef, SignallingRef[IO, Boolean]]],
    clock: Clock[IO],
    inactiveInterval: FiniteDuration,
    activeProjectsGauge: Gauge[IO, Long]
) {

  /**
    * Return the signal for the given project
    */
  def getSignal(project: ProjectRef): IO[Option[SignallingRef[IO, Boolean]]] =
    signals.get.map(_.get(project))

  private[stream] def getActiveProjects: IO[Set[ProjectRef]] =
    activeProjects.get.map(_.keySet)

  def evictInactiveProjects: IO[Unit] =
    inactivityThreshold.flatMap { inactivityInstant =>
      activeProjects.evalUpdate { projects =>
        val (active, inactive) = projects.partition { case (_, lastUpdate) =>
          lastUpdate.isAfter(inactivityInstant)
        }
        updateInactiveProjectsSignals(inactive.keys) >>
          activeProjectsGauge
            .record(active.size)
            .as(active)
      }
    }

  private def updateInactiveProjectsSignals(inactiveProjects: Iterable[ProjectRef]) =
    signals.get
      .flatMap { signals =>
        inactiveProjects.toList.traverse { project =>
          signals.get(project).traverse {
            _.set(false)
          }
        }
      }

  /**
    * Push updates for values and recompute the signals for all values with the new predicate
    */
  def newValues(updates: Seq[(ProjectRef, Instant)]): IO[Unit] =
    updates.traverse { case (project, lastInstant) =>
      inactivityThreshold.flatMap { inactivityInstant =>
        val isActive = lastInstant.isAfter(inactivityInstant)
        signals.evalUpdate { signals =>
          signals.get(project) match {
            case Some(signal) => signal.set(isActive).as(signals)
            case None         =>
              SignallingRef
                .of[IO, Boolean](isActive)
                .map(signals.updated(project, _))
          }
        } >>
          IO.whenA(isActive) {
            activeProjects.update(_.updated(project, lastInstant))
          }
      }
    }.void

  private def inactivityThreshold =
    clock.realTimeInstant.map(_.minusSeconds(inactiveInterval.toSeconds))
}

object ProjectSignals {

  private val emptySignals = Map.empty[ProjectRef, SignallingRef[IO, Boolean]]

  def apply(clock: Clock[IO], inactiveInterval: FiniteDuration)(using Meter[IO]): IO[ProjectSignals] =
    for {
      values              <- AtomicCell[IO].of(Map.empty[ProjectRef, Instant])
      signals             <- AtomicCell[IO].of(emptySignals)
      activeProjectsGauge <- Meter[IO]
                               .gauge[Long]("nexus.indexing.projects.active.gauge")
                               .withDescription("Gauge of active projects.")
                               .create
    } yield new ProjectSignals(values, signals, clock, inactiveInterval, activeProjectsGauge)

}
