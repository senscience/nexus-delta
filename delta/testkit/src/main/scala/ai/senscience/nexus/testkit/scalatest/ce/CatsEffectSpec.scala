package ai.senscience.nexus.testkit.scalatest.ce

import ai.senscience.nexus.testkit.clock.FixedClock
import ai.senscience.nexus.testkit.scalatest.{BaseSpec, ClasspathResources, ScalaTestExtractValue}
import cats.effect.IO
import org.typelevel.otel4s.trace.Tracer

trait CatsEffectSpec
    extends BaseSpec
    with CatsIOValues
    with ClasspathResources
    with ScalaTestExtractValue
    with FixedClock {

  given Tracer[IO] = Tracer.noop[IO]

}
