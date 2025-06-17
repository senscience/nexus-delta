package ai.senscience.nexus.testkit.scalatest.ce

import ai.senscience.nexus.testkit.clock.FixedClock
import ai.senscience.nexus.testkit.scalatest.{BaseSpec, ClasspathResources, ScalaTestExtractValue}

trait CatsEffectSpec
    extends BaseSpec
    with CatsIOValues
    with ClasspathResources
    with ScalaTestExtractValue
    with FixedClock
