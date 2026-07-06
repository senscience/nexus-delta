package ai.senscience.nexus.testkit.s3

import ai.senscience.nexus.testkit.TestContainers
import cats.effect.{IO, Resource}
import io.laserdisc.pure.s3.tagless.{Interpreter, S3AsyncClientOp}
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.images.builder.Transferable
import org.testcontainers.utility.DockerImageName
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient

import java.net.URI

/**
  * A lightweight, S3-compatible container backed by [[https://github.com/seaweedfs/seaweedfs SeaweedFS]] (`server
  * -s3`), used as a replacement for LocalStack in the S3 unit tests: much smaller image, faster startup, and full
  * CopyObject / metadata fidelity.
  */
class SeaweedFSContainer extends GenericContainer[SeaweedFSContainer](DockerImageName.parse(SeaweedFS.ImageName)) {
  addExposedPort(SeaweedFS.S3Port)
  withCopyToContainer(Transferable.of(SeaweedFS.s3Config), "/etc/seaweedfs/s3.json")
  setCommand("server", "-s3", "-s3.config=/etc/seaweedfs/s3.json")
  setWaitStrategy(Wait.forHttp("/").forPort(SeaweedFS.S3Port).forStatusCode(200).forStatusCode(403))
}

object SeaweedFS {
  val Version: String   = "4.37"
  val ImageName: String = s"chrislusf/seaweedfs:$Version"
  val S3Port: Int       = 8333

  val AccessKey: String = "accessKey"
  val SecretKey: String = "secretKey"

  // SeaweedFS validates signed requests against configured identities, so we register the test credentials.
  private[s3] val s3Config: String =
    s"""{
       |  "identities": [
       |    {
       |      "name": "test",
       |      "credentials": [ { "accessKey": "$AccessKey", "secretKey": "$SecretKey" } ],
       |      "actions": [ "Admin", "Read", "Write", "List", "Tagging" ]
       |    }
       |  ]
       |}""".stripMargin

  private val region      = Region.US_EAST_1
  private val credentials = StaticCredentialsProvider.create(AwsBasicCredentials.create(AccessKey, SecretKey))

  /** A running SeaweedFS container wrapped in a Resource. The container will be stopped upon release. */
  def resource(): TestContainers.ContainerResource =
    TestContainers.resource(new SeaweedFSContainer())

  /** The S3 endpoint of a running container. */
  def endpoint(container: GenericContainer[?]): URI =
    URI.create(s"http://${container.getHost}:${container.getMappedPort(S3Port)}")

  /** An fs2 S3 client pointing at the given container. */
  def fs2Client(container: GenericContainer[?]): Resource[IO, S3AsyncClientOp[IO]] =
    Interpreter[IO].S3AsyncClientOpResource(
      S3AsyncClient
        .builder()
        .credentialsProvider(credentials)
        .endpointOverride(endpoint(container))
        .forcePathStyle(true)
        .region(region)
    )
}
