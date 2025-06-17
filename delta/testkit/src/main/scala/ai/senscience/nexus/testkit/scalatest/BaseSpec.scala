package ai.senscience.nexus.testkit.scalatest

import ai.senscience.nexus.testkit.{CirceEq, Generators}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.{Inspectors, OptionValues}

trait BaseSpec
    extends AnyWordSpecLike
    with Matchers
    with EitherValues
    with OptionValues
    with Inspectors
    with TestMatchers
    with Generators
    with CirceEq
