package ai.senscience.nexus.delta.sourcing.query

import ai.senscience.nexus.delta.sourcing.Scope
import ai.senscience.nexus.delta.sourcing.config.ElemQueryConfig.{DelayConfig, PassivationConfig, StopConfig}
import ai.senscience.nexus.delta.sourcing.model.{Label, ProjectRef}
import ai.senscience.nexus.delta.sourcing.stream.ProjectActivity
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.effect.IO

import scala.concurrent.duration.DurationInt

class RefreshOrStopSuite extends NexusSuite {

  private val org               = Label.unsafe("org")
  private val project           = ProjectRef.unsafe("org", "proj")
  private val passivationConfig = PassivationConfig(20, 50.millis)

  private def activity(returnValue: Boolean): ProjectActivity =
    (_: ProjectRef) => IO.pure(returnValue)

  test("A stop config returns a stop outcome for the different scopes") {
    val stopConfig = StopConfig(20)
    val expected   = RefreshOrStop.Outcome.Stopped
    List(Scope.root, Scope.Org(org), Scope.Project(project)).foreach { scope =>
      RefreshOrStop(scope, stopConfig, ProjectActivity.noop).run(None).assertEquals(expected)
    }
  }

  test("A delay config returns a delayed outcome for the different scopes") {
    val delayConfig = DelayConfig(20, 50.millis)
    val expected    = RefreshOrStop.Outcome.Delayed
    List(Scope.root, Scope.Org(org), Scope.Project(project)).foreach { scope =>
      RefreshOrStop(scope, delayConfig, ProjectActivity.noop).run(None).assertEquals(expected)
    }
  }

  test("A passivation config fails for root and org scopes") {
    List(Scope.root, Scope.Org(org)).foreach { scope =>
      RefreshOrStop(scope, passivationConfig, ProjectActivity.noop).run(None).intercept[IllegalStateException]
    }
  }

  test("A passivation config returns a delayed outcome for an active project") {
    val expected        = RefreshOrStop.Outcome.Delayed
    val projectActivity = activity(true)
    RefreshOrStop(Scope.Project(project), passivationConfig, projectActivity).run(None).assertEquals(expected)
  }

  test("A passivation config returns a passivated outcome for an inactive project") {
    val expected        = RefreshOrStop.Outcome.Passivated
    val projectActivity = activity(false)
    RefreshOrStop(Scope.Project(project), passivationConfig, projectActivity).run(None).assertEquals(expected)
  }

  test("A passivation config returns a passivated outcome for an inactive project") {
    val expected        = RefreshOrStop.Outcome.Passivated
    val projectActivity = activity(false)
    RefreshOrStop(Scope.Project(project), passivationConfig, projectActivity).run(None).assertEquals(expected)
  }

}
