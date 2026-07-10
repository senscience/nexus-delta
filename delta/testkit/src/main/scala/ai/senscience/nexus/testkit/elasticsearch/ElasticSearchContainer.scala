package ai.senscience.nexus.testkit.elasticsearch

import ai.senscience.nexus.testkit.TestContainers
import ai.senscience.nexus.testkit.elasticsearch.ElasticSearchContainer.Version
import org.http4s.BasicCredentials
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

class ElasticSearchContainer(password: String)
    extends GenericContainer[ElasticSearchContainer](
      DockerImageName.parse(s"docker.elastic.co/elasticsearch/elasticsearch:$Version")
    ) {
  addEnv("ES_JAVA_OPTS", "-Xms2g -Xmx2g")
  addEnv("discovery.type", "single-node")
  addEnv("xpack.ml.enabled", "false")
  addEnv("xpack.watcher.enabled", "false")
  addEnv("xpack.security.enabled", "true")
  addEnv("ingest.geoip.downloader.enabled", "false")
  addEnv("ELASTIC_PASSWORD", password)
  addExposedPort(9200)
  setWaitStrategy(Wait.forHttp("/").forPort(9200).withBasicCredentials("elastic", password))
}

object ElasticSearchContainer {
  val Version = "9.4.2"

  private val ElasticSearchUser     = "elastic"
  private val ElasticSearchPassword = "password"

  val credentials: BasicCredentials = BasicCredentials(ElasticSearchUser, ElasticSearchPassword)

  /**
    * A running elasticsearch container wrapped in a Resource. The container will be stopped upon release.
    */
  def resource(): TestContainers.ContainerResource =
    TestContainers.resource(new ElasticSearchContainer(ElasticSearchPassword))
}
