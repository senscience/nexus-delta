package ai.senscience.nexus.delta.sourcing.query

import ai.senscience.nexus.delta.sourcing.Scope
import ai.senscience.nexus.delta.sourcing.config.ElemQueryConfig.{DelayConfig, PassivationConfig, StopConfig}
import ai.senscience.nexus.delta.sourcing.model.{Label, ProjectRef}
import ai.senscience.nexus.delta.sourcing.stream.ProjectActivity
import ai.senscience.nexus.testkit.mu.NexusSuite
import ai.senscience.nexus.testkit.mu.ce.PatienceConfig
import cats.effect.IO

import scala.concurrent.duration.DurationInt

class RefreshOrStopSuite extends NexusSuite {

  given PatienceConfig = PatienceConfig(5.seconds, 10.millis)

  private val org               = Label.unsafe("org")
  private val project           = ProjectRef.unsafe("org", "proj")
  private val stopConfig        = StopConfig(20, 50.millis)
  private val delayConfig       = DelayConfig(20, 50.millis)
  private val passivationConfig = PassivationConfig(20, 50.millis)

  private def activity(active: Boolean) =
    new ProjectActivity {
      override def isActive(project: ProjectRef): IO[Boolean] = IO.pure(active)
      override def activations: fs2.Stream[IO, ProjectRef]    = fs2.Stream.empty
      override def activeProjects: IO[List[ProjectRef]]       = IO.pure(List.empty)
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

  test("A passivation config returns a delayed passivation outcome when the project is active") {
    val expected = RefreshOrStop.RefreshOutcome.DelayedPassivation
    RefreshOrStop(Scope.Project(project), passivationConfig, activity(true)).run.assertEquals(expected)
  }

  test("A passivation config returns a passivated terminal outcome when the project is inactive") {
    val expected = RefreshOrStop.RefreshOutcome.Passivated
    RefreshOrStop(Scope.Project(project), passivationConfig, activity(false)).run.assertEquals(expected)
  }

  test("An unknown project (no activity tracked) passivates") {
    val expected = RefreshOrStop.RefreshOutcome.Passivated
    RefreshOrStop(Scope.Project(project), passivationConfig, ProjectActivity.noop).run.assertEquals(expected)
  }

  test("Passivated is terminal and Continue outcomes are not") {
    assert(RefreshOrStop.RefreshOutcome.Passivated.isTerminal)
    assert(RefreshOrStop.RefreshOutcome.Stopped.isTerminal)
    assert(!RefreshOrStop.RefreshOutcome.Delayed.isTerminal)
    assert(!RefreshOrStop.RefreshOutcome.DelayedPassivation.isTerminal)
  }

}
