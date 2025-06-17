package ai.senscience.nexus.delta.sdk.utils

import ai.senscience.nexus.delta.sdk.ConfigFixtures
import ai.senscience.nexus.delta.sourcing.postgres.DoobieScalaTestFixture
import ch.epfl.bluebrain.nexus.testkit.*
import ch.epfl.bluebrain.nexus.testkit.ce.IOFromMap
import ch.epfl.bluebrain.nexus.testkit.scalatest.TestMatchers
import ch.epfl.bluebrain.nexus.testkit.scalatest.ce.{CatsEffectSpec, CatsIOValues}
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Inspectors, OptionValues}

trait BaseRouteSpec
    extends CatsEffectSpec
    with RouteHelpers
    with DoobieScalaTestFixture
    with Matchers
    with CatsIOValues
    with IOFromMap
    with CirceLiteral
    with CirceEq
    with OptionValues
    with TestMatchers
    with Inspectors
    with ConfigFixtures
    with RouteFixtures {}
