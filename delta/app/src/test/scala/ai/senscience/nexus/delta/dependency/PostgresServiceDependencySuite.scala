package ai.senscience.nexus.delta.dependency

import ai.senscience.nexus.delta.kernel.dependency.ComponentDescription.ServiceDescription
import ai.senscience.nexus.delta.sourcing.postgres.Doobie
import ai.senscience.nexus.testkit.mu.NexusSuite

class PostgresServiceDependencySuite extends NexusSuite with Doobie.Fixture {

  test("fetch its service name and version") {
    val xas = doobie()
    new PostgresServiceDependency(xas).serviceDescription.assertEquals(ServiceDescription("postgres", "17.6"))
  }

}
