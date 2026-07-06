package ai.senscience.nexus.testkit.postgres

import ai.senscience.nexus.testkit.TestContainers
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

class PostgresContainer(user: String, password: String, database: String)
    extends GenericContainer[PostgresContainer](DockerImageName.parse("library/postgres:18.4")) {
  addEnv("POSTGRES_USER", user)
  addEnv("POSTGRES_PASSWORD", password)
  addEnv("POSTGRES_DB", database)
  addExposedPort(5432)
  setWaitStrategy(Wait.forLogMessage(".*database system is ready to accept connections.*\\s", 2))
  setCommand("postgres", "-c", "fsync=off")
}

object PostgresContainer {

  /**
    * A running postgres container wrapped in a Resource. The container will be stopped upon release.
    */
  def resource(user: String, password: String, database: String): TestContainers.ContainerResource =
    TestContainers.resource(new PostgresContainer(user, password, database))

}
