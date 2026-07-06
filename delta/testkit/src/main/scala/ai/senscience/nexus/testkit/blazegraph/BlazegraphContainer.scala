package ai.senscience.nexus.testkit.blazegraph

import ai.senscience.nexus.testkit.TestContainers
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

class BlazegraphContainer
    extends GenericContainer[BlazegraphContainer](DockerImageName.parse("bluebrain/blazegraph-nexus:2.1.6-RC-21-jre")) {
  addEnv("JAVA_OPTS", "-Djava.awt.headless=true -XX:MaxDirectMemorySize=64m -Xmx512m -XX:+UseG1GC")
  addExposedPort(9999)
  setWaitStrategy(Wait.forHttp("/blazegraph").forStatusCode(200))
}

object BlazegraphContainer {

  /**
    * A running blazegraph container wrapped in a Resource. The container will be stopped upon release.
    */
  def resource(): TestContainers.ContainerResource =
    TestContainers.resource(new BlazegraphContainer())

}
