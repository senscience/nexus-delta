package ai.senscience.nexus.delta.dependency

import ai.senscience.nexus.delta.sourcing.postgres.{DoobieScalaTestFixture, PostgresDocker}
import ai.senscience.nexus.testkit.scalatest.ce.CatsEffectSpec
import ch.epfl.bluebrain.nexus.delta.kernel.dependency.ComponentDescription.ServiceDescription

class PostgresServiceDependencySpec extends CatsEffectSpec with DoobieScalaTestFixture with PostgresDocker {

  "PostgresServiceDependency" should {

    "fetch its service name and version" in {
      new PostgresServiceDependency(xas).serviceDescription.accepted shouldEqual ServiceDescription("postgres", "17.5")
    }
  }

}
