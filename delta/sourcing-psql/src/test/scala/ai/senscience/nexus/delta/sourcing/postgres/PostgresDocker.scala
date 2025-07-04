package ai.senscience.nexus.delta.sourcing.postgres

import ai.senscience.nexus.delta.sourcing.postgres.PostgresDocker.PostgresHostConfig
import ai.senscience.nexus.testkit.postgres.PostgresContainer
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.concurrent.duration.*
import scala.jdk.DurationConverters.*

trait PostgresDocker extends BeforeAndAfterAll { this: Suite =>

  protected val container: PostgresContainer =
    new PostgresContainer(PostgresUser, PostgresPassword, PostgresDb)
      .withReuse(false)
      .withStartupTimeout(60.seconds.toJava)

  def hostConfig: PostgresHostConfig =
    PostgresHostConfig(container.getHost, container.getMappedPort(5432))

  override def beforeAll(): Unit = {
    super.beforeAll()
    container.start()
  }

  override def afterAll(): Unit = {
    container.stop()
    super.afterAll()
  }

}

object PostgresDocker {
  final case class PostgresHostConfig(host: String, port: Int)
}
