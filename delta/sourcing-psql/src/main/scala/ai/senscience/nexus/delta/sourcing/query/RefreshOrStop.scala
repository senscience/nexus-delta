package ai.senscience.nexus.delta.sourcing.query

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.sourcing.Scope
import ai.senscience.nexus.delta.sourcing.config.ElemQueryConfig
import ai.senscience.nexus.delta.sourcing.config.ElemQueryConfig.{DelayConfig, PassivationConfig, StopConfig}
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.query.RefreshOrStop.RefreshOutcome
import ai.senscience.nexus.delta.sourcing.query.RefreshOrStop.RefreshOutcome.*
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
    case object Stopped            extends RefreshOutcome
    sealed trait Continue          extends RefreshOutcome
    case object Delayed            extends Continue
    case object NoSignal           extends Continue
    case object DelayedPassivation extends Continue
    case object Passivated         extends Continue
  }

  private val logger = Logger[RefreshOrStop.type]

  def apply(scope: Scope, config: ElemQueryConfig, projectActivity: ProjectActivity): RefreshOrStop =
    new RefreshOrStop {
      override def run: IO[RefreshOutcome] = {
        (config, scope) match {
          case (_: StopConfig, _)                             => IO.pure(Stopped)
          case (d: DelayConfig, _)                            => IO.sleep(d.delay).as(Delayed)
          case (w: PassivationConfig, Scope.Project(project)) =>
            projectActivity.apply(project).flatMap {
              case Some(signal) =>
                signal.get.flatMap {
                  case true  =>
                    logger.debug(s"Project '$project' is active, continue after ${w.delay}") >>
                      IO.sleep(w.delay).as(DelayedPassivation)
                  case false => passivate(project, signal)
                }
              case None         =>
                logger.debug(s"No signal has been found for project '$project', continue after ${w.delay}") >> IO
                  .sleep(w.delay)
                  .as(NoSignal)
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
    } yield Passivated

}
