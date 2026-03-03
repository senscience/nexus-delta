package ai.senscience.nexus.delta.sourcing.query

import ai.senscience.nexus.delta.sourcing.Scope
import ai.senscience.nexus.delta.sourcing.config.ElemQueryConfig.{DelayConfig, PassivationConfig, StopConfig}
import ai.senscience.nexus.delta.sourcing.model.{Label, ProjectRef}
import ai.senscience.nexus.delta.sourcing.stream.ProjectActivity
import ai.senscience.nexus.testkit.mu.NexusSuite
import ai.senscience.nexus.testkit.mu.ce.PatienceConfig
import cats.effect.{IO, Ref}
import fs2.concurrent.SignallingRef

import scala.concurrent.duration.DurationInt

class RefreshOrStopSuite extends NexusSuite {

  given PatienceConfig = PatienceConfig(5.seconds, 10.millis)

  private val org               = Label.unsafe("org")
  private val project           = ProjectRef.unsafe("org", "proj")
  private val stopConfig        = StopConfig(20, 50.millis)
  private val delayConfig       = DelayConfig(20, 50.millis)
  private val passivationConfig = PassivationConfig(20, 50.millis)

  private def activity(signal: SignallingRef[IO, Boolean]) =
    new ProjectActivity {
      override def apply(project: ProjectRef): IO[Option[SignallingRef[IO, Boolean]]] = IO.some(signal)
    }

  test("A stop config returns a stop outcome for the different scopes") {
    val expected = RefreshOrStop.RefreshOutcome.Stopped
    for {
      _ <- RefreshOrStop(Scope.root, stopConfig, ProjectActivity.noop).run.assertEquals(expected)
      _ <- RefreshOrStop(Scope.Org(org), stopConfig, ProjectActivity.noop).run.assertEquals(expected)
      _ <- RefreshOrStop(Scope.Project(project), stopConfig, ProjectActivity.noop).run.assertEquals(expected)
    } yield ()
  }

  test("A delay config returns a delayed outcome for the different scopes") {
    val expected = RefreshOrStop.RefreshOutcome.Delayed
    for {
      _ <- RefreshOrStop(Scope.root, delayConfig, ProjectActivity.noop).run.assertEquals(expected)
      _ <- RefreshOrStop(Scope.Org(org), delayConfig, ProjectActivity.noop).run.assertEquals(expected)
      _ <- RefreshOrStop(Scope.Project(project), delayConfig, ProjectActivity.noop).run.assertEquals(expected)
    } yield ()
  }

  test("A passivation config fails for root and org scopes") {
    for {
      _ <-
        RefreshOrStop(Scope.root, passivationConfig, ProjectActivity.noop).run.intercept[IllegalStateException]
      _ <- RefreshOrStop(Scope.Org(org), passivationConfig, ProjectActivity.noop).run
             .intercept[IllegalStateException]
    } yield ()
  }

  test("A passivation config returns a no signal outcome when no signal is available") {
    val expected = RefreshOrStop.RefreshOutcome.NoSignal
    RefreshOrStop(Scope.Project(project), passivationConfig, ProjectActivity.noop).run.assertEquals(expected)
  }

  test("A passivation config returns a delayed passivation outcome when no signal is available") {
    val expected = RefreshOrStop.RefreshOutcome.DelayedPassivation
    for {
      signal         <- SignallingRef.of[IO, Boolean](true)
      projectActivity = activity(signal)
      _              <- RefreshOrStop(Scope.Project(project), passivationConfig, projectActivity).run.assertEquals(expected)
    } yield ()
  }

  test("A passivation config returns eventually a passivated outcome when a signal gets activated") {
    val expected = RefreshOrStop.RefreshOutcome.Passivated
    for {
      signal         <- SignallingRef.of[IO, Boolean](false)
      obtained       <- Ref.of[IO, Option[RefreshOrStop.RefreshOutcome]](None)
      projectActivity = activity(signal)
      _              <- RefreshOrStop(Scope.Project(project), passivationConfig, projectActivity).run.flatTap { outcome =>
                          obtained.set(Some(outcome))
                        }.start
      _              <- signal.set(true)
      _              <- obtained.get.assertEquals(Some(expected)).eventually
    } yield ()
  }

}
