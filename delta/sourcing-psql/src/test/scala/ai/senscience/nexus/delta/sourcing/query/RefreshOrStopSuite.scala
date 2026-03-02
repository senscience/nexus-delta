package ai.senscience.nexus.delta.sourcing.query

import ai.senscience.nexus.delta.sourcing.Scope
import ai.senscience.nexus.delta.sourcing.config.ElemQueryConfig.{DelayConfig, PassivationConfig, StopConfig}
import ai.senscience.nexus.delta.sourcing.model.{Label, ProjectRef}
import ai.senscience.nexus.delta.sourcing.query.RefreshOrStop.RefreshOutcome
import ai.senscience.nexus.delta.sourcing.query.RefreshOrStop.RefreshOutcome.Reason
import ai.senscience.nexus.delta.sourcing.stream.ProjectActivity
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.effect.IO
import fs2.concurrent.SignallingRef
import munit.Location

import scala.concurrent.duration.DurationInt

class RefreshOrStopSuite extends NexusSuite {

  private val org               = Label.unsafe("org")
  private val project           = ProjectRef.unsafe("org", "proj")
  private val passivationConfig = PassivationConfig(20, 20, 50.millis)

  private def assertReason(obtained: RefreshOutcome, expectedReason: Reason)(using location: Location): Unit =
    obtained match {
      case RefreshOutcome.Stopped             => fail(s"Got stopped, expecting continue with reason $expectedReason")
      case RefreshOutcome.Continue(_, reason) => assertEquals(reason, expectedReason)
    }

  private def activity(returnValue: Boolean): ProjectActivity =
    (_: ProjectRef) => SignallingRef.of[IO, Boolean](returnValue).flatMap(IO.some)
  test("A stop config returns a stop outcome for the different scopes") {
    val stopConfig = StopConfig(20, 20, 50.millis)
    val expected   = RefreshOrStop.RefreshOutcome.Stopped
    List(Scope.root, Scope.Org(org), Scope.Project(project)).foreach { scope =>
      RefreshOrStop(scope, stopConfig, ProjectActivity.noop).run.assertEquals(expected)
    }
  }

  test("A delay config returns a delayed outcome for the different scopes") {
    val delayConfig = DelayConfig(20, 20, 50.millis)
    List(Scope.root, Scope.Org(org), Scope.Project(project)).foreach { scope =>
      RefreshOrStop(scope, delayConfig, ProjectActivity.noop).run.map(assertReason(_, Reason.Delayed))
    }
  }

  test("A passivation config fails for root and org scopes") {
    List(Scope.root, Scope.Org(org)).foreach { scope =>
      RefreshOrStop(scope, passivationConfig, ProjectActivity.noop).run.intercept[IllegalStateException]
    }
  }

  test("A passivation config returns a delayed outcome for an active project") {
    val projectActivity = activity(true)
    RefreshOrStop(Scope.Project(project), passivationConfig, projectActivity).run
      .map(assertReason(_, Reason.DelayedPassivation))
  }

  test("A passivation config returns a passivated outcome for an inactive project") {
    val projectActivity = activity(false)
    RefreshOrStop(Scope.Project(project), passivationConfig, projectActivity).run
      .map(assertReason(_, Reason.Passivated))
  }

}
