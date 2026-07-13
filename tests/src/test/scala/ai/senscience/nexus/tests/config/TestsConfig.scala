package ai.senscience.nexus.tests.config

import ai.senscience.nexus.tests.Realm
import org.apache.pekko.http.scaladsl.model.Uri
import pureconfig.ConvertHelpers.catchReadError
import pureconfig.{ConfigConvert, ConfigReader}
import pureconfig.generic.semiauto.deriveReader

import scala.concurrent.duration.FiniteDuration

case class TestsConfig(
    deltaUri: Uri,
    realmUri: Uri,
    patience: FiniteDuration,
    cleanUp: Boolean,
    elasticsearch: TestsConfig.ElasticsearchConfig
) {

  def realmSuffix(realm: Realm) = s"$realmUri/${realm.name}"
}

object TestsConfig {
  final case class S3Config(accessKey: Option[String], secretKey: Option[String], prefix: String)

  final case class StorageConfig(s3: S3Config, maxFileSize: Long)

  /**
    * Configuration of the Elasticsearch cluster the integration tests assert against.
    *
    * @param apiKey
    *   when defined, an `ApiKey` Authorization header is used instead of the basic auth credentials
    */
  final case class ElasticsearchConfig(
      base: Uri,
      serverless: Boolean,
      username: String,
      password: String,
      apiKey: Option[String]
  )

  given ConfigConvert[Uri] = ConfigConvert.viaString[Uri](catchReadError(s => Uri(s)), _.toString)

  given ConfigReader[S3Config] = deriveReader[S3Config]

  given ConfigReader[StorageConfig] = deriveReader[StorageConfig]

  given ConfigReader[ElasticsearchConfig] = deriveReader[ElasticsearchConfig]

  given ConfigReader[TestsConfig] = deriveReader[TestsConfig]
}
