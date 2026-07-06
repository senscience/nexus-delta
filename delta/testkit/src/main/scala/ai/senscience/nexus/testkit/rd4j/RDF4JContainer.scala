package ai.senscience.nexus.testkit.rd4j

import ai.senscience.nexus.testkit.TestContainers
import ai.senscience.nexus.testkit.rd4j.RDF4JContainer.ImageName
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

class RDF4JContainer extends GenericContainer[RDF4JContainer](DockerImageName.parse(ImageName)) {
  addExposedPort(8080)
  setWaitStrategy(Wait.forHttp("/rdf4j-server").forStatusCode(200))
}

object RDF4JContainer {

  private val ImageName = "eclipse/rdf4j-workbench:5.1.2"

  /**
    * A running RDF4J container wrapped in a Resource. The container will be stopped upon release.
    */
  def resource(): TestContainers.ContainerResource =
    TestContainers.resource(new RDF4JContainer())

}
