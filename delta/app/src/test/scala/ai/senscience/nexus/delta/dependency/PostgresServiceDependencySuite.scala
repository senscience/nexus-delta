package ai.senscience.nexus.delta.dependency

import ai.senscience.nexus.delta.kernel.dependency.ComponentDescription.ServiceDescription
import ai.senscience.nexus.delta.sourcing.postgres.Doobie
import ai.senscience.nexus.testkit.mu.NexusSuite
import munit.AnyFixture

class PostgresServiceDependencySuite extends NexusSuite with Doobie.Fixture {

  override def munitFixtures: Seq[AnyFixture[?]] = List(doobie)

  test("fetch its service name and version") {
    val xas = doobie()
    new PostgresServiceDependency(xas).serviceDescription.assertEquals(ServiceDescription("postgres", "18.1"))
  }

}
