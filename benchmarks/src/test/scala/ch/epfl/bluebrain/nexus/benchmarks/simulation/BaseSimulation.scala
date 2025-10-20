package ch.epfl.bluebrain.nexus.benchmarks.simulation

import io.gatling.core.Predef.*
import io.gatling.core.structure.ScenarioBuilder
import io.gatling.http.Predef.*

abstract class BaseSimulation extends Simulation {

  def setupSimulation(scn: ScenarioBuilder, baseUrl: String, users: Int): SetUp = {
    val httpProtocol = http.baseUrl(baseUrl)
    setUp(
      scn
        .inject(atOnceUsers(users))
        .protocols(httpProtocol)
    )
  }

}

object BaseSimulation
