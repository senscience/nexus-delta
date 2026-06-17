package ai.senscience.nexus.delta.sourcing.query

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.sourcing.Scope
import ai.senscience.nexus.delta.sourcing.config.ElemQueryConfig
import ai.senscience.nexus.delta.sourcing.config.ElemQueryConfig.{DelayConfig, PassivationConfig, StopConfig}
import ai.senscience.nexus.delta.sourcing.query.RefreshOrStop.RefreshOutcome
import ai.senscience.nexus.delta.sourcing.query.RefreshOrStop.RefreshOutcome.*
import ai.senscience.nexus.delta.sourcing.stream.ProjectActivity
import cats.effect.IO

/**
  * Computes the outcome to apply when all elements are consumed by a projection
  */
trait RefreshOrStop {
  def run: IO[RefreshOutcome]
}

object RefreshOrStop {

  sealed trait RefreshOutcome {

    /**
      * @return
      *   true if the outcome is terminal and the projection stream should stop
      */
    def isTerminal: Boolean
  }

  object RefreshOutcome {
    sealed trait Terminal extends RefreshOutcome {
      override def isTerminal: Boolean = true
    }
    sealed trait Continue extends RefreshOutcome {
      override def isTerminal: Boolean = false
    }

    case object Stopped            extends Terminal
    case object Passivated         extends Terminal
    case object Delayed            extends Continue
    case object DelayedPassivation extends Continue
  }

  private val logger = Logger[RefreshOrStop.type]

  def apply(scope: Scope, config: ElemQueryConfig, projectActivity: ProjectActivity): RefreshOrStop =
    new RefreshOrStop {
      override def run: IO[RefreshOutcome] = {
        (config, scope) match {
          case (_: StopConfig, _)                             => IO.pure(Stopped)
          case (d: DelayConfig, _)                            => IO.sleep(d.delay).as(Delayed)
          case (w: PassivationConfig, Scope.Project(project)) =>
            projectActivity.isActive(project).flatMap {
              case true  =>
                logger.debug(s"Project '$project' is active, continue after ${w.delay}") >>
                  IO.sleep(w.delay).as(DelayedPassivation)
              case false =>
                logger
                  .debug(s"Project '$project' is inactive, stopping projection until activity resumes.")
                  .as(Passivated)
            }
          case (c, s)                                         =>
            // Passivation is only available at the project scope
            IO.raiseError(new IllegalStateException(s"'$c' and '$s' is not a valid combination, it should not happen"))
        }
      }
    }
}
