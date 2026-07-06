package ai.senscience.nexus.delta.sourcing.postgres

import ai.senscience.nexus.delta.sourcing.Transactors
import ai.senscience.nexus.testkit.Generators
import ai.senscience.nexus.testkit.clock.FixedClock
import ai.senscience.nexus.testkit.scalatest.ce.CatsIOValues
import cats.effect.IO
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.compiletime.uninitialized

trait DoobieScalaTestFixture extends BeforeAndAfterAll with Generators with CatsIOValues with FixedClock {

  self: Suite =>

  var xas: Transactors              = uninitialized
  private var xasTeardown: IO[Unit] = uninitialized

  override def beforeAll(): Unit = {
    super.beforeAll()
    val (x, t) = Doobie.resourceDefault.allocated.accepted
    xas = x
    xasTeardown = t
  }

  override def afterAll(): Unit = {
    xasTeardown.accepted
    super.afterAll()
  }

}
