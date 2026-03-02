package ai.senscience.nexus.delta.sourcing.query

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.sourcing.Scope
import ai.senscience.nexus.delta.sourcing.config.ElemQueryConfig
import ai.senscience.nexus.delta.sourcing.config.ElemQueryConfig.{DelayConfig, PassivationConfig, StopConfig}
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.query.RefreshOrStop.RefreshOutcome
import ai.senscience.nexus.delta.sourcing.query.RefreshOrStop.RefreshOutcome.*
import ai.senscience.nexus.delta.sourcing.query.RefreshOrStop.RefreshOutcome.Reason.Delayed
import ai.senscience.nexus.delta.sourcing.stream.ProjectActivity
import cats.effect.IO
import fs2.Stream
import fs2.concurrent.SignallingRef

/**
  * Computes the outcome to apply when all elements are consumed by a projection
  */
trait RefreshOrStop {
  def run: IO[RefreshOutcome]
}

object RefreshOrStop {

  sealed trait RefreshOutcome

  object RefreshOutcome {
    enum Reason {
      case Delayed
      case NoSignal
      case DelayedPassivation
      case Passivated
    }

    case object Stopped                                         extends RefreshOutcome
    final case class Continue(action: IO[Unit], reason: Reason) extends RefreshOutcome
  }

  private val logger = Logger[RefreshOrStop]

  def apply(scope: Scope, config: ElemQueryConfig, projectActivity: ProjectActivity): RefreshOrStop =
    new RefreshOrStop {
      override def run: IO[RefreshOutcome] = {
        (config, scope) match {
          case (_: StopConfig, _)                             => IO.pure(Stopped)
          case (d: DelayConfig, _)                            => IO.pure(Continue(IO.sleep(d.delay), Delayed))
          case (w: PassivationConfig, Scope.Project(project)) =>
            projectActivity(project).flatMap {
              case Some(signal) =>
                signal.get.flatMap {
                  case true  =>
                    logger
                      .debug(s"Project '$project' is active, continue after ${w.delay}")
                      .as(Continue(IO.sleep(w.delay), Reason.DelayedPassivation))
                  case false => IO.pure(Continue(passivate(project, signal), Reason.Passivated))
                }
              case None         =>
                logger
                  .debug(s"No signal has been found for project '$project', continue after ${w.delay}")
                  .as(Continue(IO.sleep(w.delay), Reason.NoSignal))
            }
          case (c, s)                                         =>
            // Passivation is only available at the project scope
            IO.raiseError(new IllegalStateException(s"'$c' and '$s' is not a valid combination, it should not happen"))
        }
      }
    }

  private def passivate(project: ProjectRef, signal: SignallingRef[IO, Boolean]) =
    for {
      _        <- logger.info(s"Project '$project' is inactive, pausing until some activity is seen again.")
      duration <- Stream.never[IO].interruptWhen(signal).compile.drain.timed.map(_._1.toCoarsest)
      _        <- logger.info(s"Project '$project' is active again after `$duration`, querying will resume.")
    } yield ()

}
