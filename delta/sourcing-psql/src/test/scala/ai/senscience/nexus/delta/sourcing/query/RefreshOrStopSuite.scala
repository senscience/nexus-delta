package ai.senscience.nexus.delta.sourcing.query

import ai.senscience.nexus.delta.sourcing.Scope
import ai.senscience.nexus.delta.sourcing.config.ElemQueryConfig.{DelayConfig, PassivationConfig, StopConfig}
import ai.senscience.nexus.delta.sourcing.model.{Label, ProjectRef}
import ai.senscience.nexus.delta.sourcing.stream.ProjectActivitySignals
import ai.senscience.nexus.testkit.mu.NexusSuite
import ai.senscience.nexus.testkit.mu.ce.PatienceConfig
import cats.effect.{IO, Ref}
import fs2.concurrent.SignallingRef

import scala.concurrent.duration.DurationInt

class RefreshOrStopSuite extends NexusSuite {

  private given PatienceConfig = PatienceConfig(5.seconds, 10.millis)

  private val org               = Label.unsafe("org")
  private val project           = ProjectRef.unsafe("org", "proj")
  private val stopConfig        = StopConfig(20)
  private val delayConfig       = DelayConfig(20, 50.millis)
  private val passivationConfig = PassivationConfig(20, 50.millis)

  private def activitySignals(signal: SignallingRef[IO, Boolean]): ProjectActivitySignals =
    (_: ProjectRef) => IO.some(signal)

  test("A stop config returns a stop outcome for the different scopes") {
    val expected = RefreshOrStop.Outcome.Stopped
    for {
      _ <- RefreshOrStop(Scope.root, stopConfig, ProjectActivitySignals.noop).run.assertEquals(expected)
      _ <- RefreshOrStop(Scope.Org(org), stopConfig, ProjectActivitySignals.noop).run.assertEquals(expected)
      _ <- RefreshOrStop(Scope.Project(project), stopConfig, ProjectActivitySignals.noop).run.assertEquals(expected)
    } yield ()
  }

  test("A delay config returns a delayed outcome for the different scopes") {
    val expected = RefreshOrStop.Outcome.Delayed
    for {
      _ <- RefreshOrStop(Scope.root, delayConfig, ProjectActivitySignals.noop).run.assertEquals(expected)
      _ <- RefreshOrStop(Scope.Org(org), delayConfig, ProjectActivitySignals.noop).run.assertEquals(expected)
      _ <- RefreshOrStop(Scope.Project(project), delayConfig, ProjectActivitySignals.noop).run.assertEquals(expected)
    } yield ()
  }

  test("A passivation config fails for root and org scopes") {
    for {
      _ <-
        RefreshOrStop(Scope.root, passivationConfig, ProjectActivitySignals.noop).run.intercept[IllegalStateException]
      _ <- RefreshOrStop(Scope.Org(org), passivationConfig, ProjectActivitySignals.noop).run
             .intercept[IllegalStateException]
    } yield ()
  }

  test("A passivation config returns a no signal outcome when no signal is available") {
    val expected = RefreshOrStop.Outcome.NoSignal
    RefreshOrStop(Scope.Project(project), passivationConfig, ProjectActivitySignals.noop).run.assertEquals(expected)
  }

  test("A passivation config returns a delayed passivation outcome when no signal is available") {
    val expected = RefreshOrStop.Outcome.DelayedPassivation
    for {
      signal               <- SignallingRef.of[IO, Boolean](true)
      projectActivitySignal = activitySignals(signal)
      _                    <- RefreshOrStop(Scope.Project(project), passivationConfig, projectActivitySignal).run.assertEquals(expected)
    } yield ()
  }

  test("A passivation config returns eventually a passivated outcome when a signal gets activated") {
    val expected = RefreshOrStop.Outcome.Passivated
    for {
      signal               <- SignallingRef.of[IO, Boolean](false)
      obtained             <- Ref.of[IO, Option[RefreshOrStop.Outcome]](None)
      projectActivitySignal = activitySignals(signal)
      _                    <- RefreshOrStop(Scope.Project(project), passivationConfig, projectActivitySignal).run.flatTap { outcome =>
                                obtained.set(Some(outcome))
                              }.start
      _                    <- signal.set(true)
      _                    <- obtained.get.assertEquals(Some(expected)).eventually
    } yield ()
  }

}
