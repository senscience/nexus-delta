package ai.senscience.nexus.testkit.mu

import ai.senscience.nexus.testkit.clock.FixedClock
import ai.senscience.nexus.testkit.mu.ce.{CatsEffectEventually, CatsIOValues, MoreCatsEffectAssertions}
import ai.senscience.nexus.testkit.scalatest.{ClasspathResources, MUnitExtractValue}
import ai.senscience.nexus.testkit.{CirceLiteral, Generators}
import cats.effect.IO
import munit.CatsEffectSuite
import org.typelevel.otel4s.trace.Tracer

abstract class NexusSuite
    extends CatsEffectSuite
    with MoreCatsEffectAssertions
    with CollectionAssertions
    with EitherAssertions
    with Generators
    with CirceLiteral
    with EitherValues
    with MUnitExtractValue
    with ClasspathResources
    with CatsIOValues
    with StreamAssertions
    with CatsEffectEventually
    with FixedClock {

  given Tracer[IO] = Tracer.noop[IO]
}
