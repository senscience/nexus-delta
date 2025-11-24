package ai.senscience.nexus.delta.sdk.utils

import ai.senscience.nexus.delta.sdk.ConfigFixtures
import ai.senscience.nexus.testkit.scalatest.TestMatchers
import ai.senscience.nexus.testkit.scalatest.ce.{CatsEffectSpec, CatsIOValues}
import ai.senscience.nexus.testkit.{CirceEq, CirceLiteral}
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Inspectors, OptionValues}

trait BaseRouteSpec
    extends CatsEffectSpec
    with RouteHelpers
    with Matchers
    with CatsIOValues
    with CirceLiteral
    with CirceEq
    with OptionValues
    with TestMatchers
    with Inspectors
    with ConfigFixtures
    with RouteFixtures {}
