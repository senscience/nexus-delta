package ai.senscience.nexus.delta.sourcing.query

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.sourcing.Scope
import ai.senscience.nexus.delta.sourcing.config.ElemQueryConfig
import ai.senscience.nexus.delta.sourcing.config.ElemQueryConfig.{DelayConfig, PassivationConfig, StopConfig}
import ai.senscience.nexus.delta.sourcing.query.RefreshOrStop.Outcome
import ai.senscience.nexus.delta.sourcing.query.RefreshOrStop.Outcome.*
import ai.senscience.nexus.delta.sourcing.stream.ProjectActivity
import cats.effect.IO

/**
  * Computes the outcome to apply when all elements are consumed by a projection
  */
trait RefreshOrStop {
  def run(previous: Option[Outcome]): IO[Outcome]
}

object RefreshOrStop {

  sealed trait Outcome

  object Outcome {
    case object Stopped          extends Outcome
    sealed trait Continue        extends Outcome
    case object Delayed          extends Continue
    case object OutOfPassivation extends Continue
    case object Passivated       extends Continue
  }

  private val logger = Logger[RefreshOrStop]

  def apply(scope: Scope, config: ElemQueryConfig, projectActivity: ProjectActivity): RefreshOrStop =
    new RefreshOrStop {
      override def run(previous: Option[Outcome]): IO[Outcome] = {
        (config, scope) match {
          case (_: StopConfig, _)                             => IO.pure(Stopped)
          case (d: DelayConfig, _)                            => IO.sleep(d.delay).as(Delayed)
          case (w: PassivationConfig, Scope.Project(project)) =>
            projectActivity(project).flatMap {
              case true  =>
                previous match {
                  case Some(Passivated) =>
                    logger.debug(s"Project '$project' is active again, querying will resume.") >>
                      IO.pure(OutOfPassivation)
                  case _                =>
                    logger.debug(s"Project '$project' is active, continue after ${w.delay}") >>
                      IO.sleep(w.delay).as(Delayed)
                }
              case false =>
                logger.debug(s"Project '$project' is inactive, pausing until some activity is seen again.") >>
                  IO.sleep(w.delay).as(Passivated)
            }
          case (c, s)                                         =>
            // Passivation is only available at the project scope
            IO.raiseError(new IllegalStateException(s"'$c' and '$s' is not a valid combination, it should not happen"))
        }
      }
    }

}
