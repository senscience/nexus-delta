package ai.senscience.nexus.delta.elasticsearch.configured

import ai.senscience.nexus.delta.elasticsearch.client.IndexLabel
import ai.senscience.nexus.delta.elasticsearch.configured.ConfiguredIndexingConfig.ConfiguredIndexingConfigError.{InvalidType, NoIndexDefined}
import ai.senscience.nexus.delta.elasticsearch.model.ElasticsearchIndexDef
import ai.senscience.nexus.delta.kernel.config.*
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sdk.error.SDKError
import cats.data.NonEmptyList
import cats.effect.IO
import cats.syntax.all.*
import com.typesafe.config.Config

import scala.jdk.CollectionConverters.*

sealed trait ConfiguredIndexingConfig

object ConfiguredIndexingConfig {

  abstract class ConfiguredIndexingConfigError extends SDKError

  object ConfiguredIndexingConfigError {

    case object NoIndexDefined extends SDKError {
      override def getMessage: String = "At least one index must be defined"
    }

    final case class InvalidType(input: String, error: String) extends SDKError {
      override def getMessage: String = s"$input is not a valid IRI"
    }
  }

  case object Disabled extends ConfiguredIndexingConfig

  final case class ConfiguredIndex(index: IndexLabel, indexDef: ElasticsearchIndexDef, types: Set[Iri]) {
    def prefixedIndex(prefix: String): IndexLabel =
      index.copy(s"${prefix}_$index")
  }

  final case class Enabled(prefix: String, indices: NonEmptyList[ConfiguredIndex]) extends ConfiguredIndexingConfig

  private def loadConfiguredIndex(indexConfig: Config): IO[ConfiguredIndex] = {

    for {
      index    <- IO.fromEither(IndexLabel(indexConfig.getString("index")))
      types    <- IO.fromEither(
                    indexConfig.getStringList("types").asScala.toList.traverse(s => Iri(s).leftMap(InvalidType(s, _)))
                  )
      indexDef <- ElasticsearchIndexDef.fromExternalFiles(
                    indexConfig.getFilePath("mapping"),
                    indexConfig.getOptionalFilePath("settings")
                  )
    } yield ConfiguredIndex(index, indexDef, types.toSet)
  }

  def load(config: Config): IO[ConfiguredIndexingConfig] = {
    val subConfig = config.getConfig("app.elasticsearch.configured-indexing")
    if subConfig.getBoolean("enabled") then {
      val prefix = subConfig.getString("prefix")
      subConfig
        .getConfigList("values")
        .asScala
        .toList
        .traverse { indexConfig =>
          loadConfiguredIndex(indexConfig)
        }
        .flatMap { indices =>
          IO.fromOption(NonEmptyList.fromList(indices))(NoIndexDefined).map(Enabled(prefix, _))
        }
    } else IO.pure(Disabled)

  }
}
