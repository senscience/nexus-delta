package ai.senscience.nexus.delta.elasticsearch.indexing

import ai.senscience.nexus.delta.elasticsearch.client.{ElasticSearchClient, Refresh}
import ai.senscience.nexus.delta.elasticsearch.configured.{ConfiguredElasticSink, ConfiguredIndexingConfig}
import ai.senscience.nexus.delta.sdk.indexing.ProjectProjectionFactory
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.stream.AnnotatedSourceStream
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.stream.config.BatchConfig
import ai.senscience.nexus.delta.sourcing.stream.{CompiledProjection, ExecutionStrategy, Source}
import cats.data.NonEmptyChain
import cats.effect.IO
import cats.syntax.all.*
import org.typelevel.otel4s.trace.Tracer

object ConfiguredIndexingProjectionFactory {

  def apply(
      annotatedSourceStream: AnnotatedSourceStream,
      client: ElasticSearchClient,
      configuredIndexing: ConfiguredIndexingConfig,
      batch: BatchConfig,
      indexingEnabled: Boolean
  )(using BaseUri, Tracer[IO]): Option[ProjectProjectionFactory] = {
    (indexingEnabled, configuredIndexing) match {
      case (false, _)                                    => None
      case (_, ConfiguredIndexingConfig.Disabled)        => None
      case (_, config: ConfiguredIndexingConfig.Enabled) =>
        Some(apply(annotatedSourceStream, client, config, batch))
    }

  }

  private def apply(
      annotatedSourceStream: AnnotatedSourceStream,
      client: ElasticSearchClient,
      config: ConfiguredIndexingConfig.Enabled,
      batch: BatchConfig
  )(using BaseUri, Tracer[IO]): ProjectProjectionFactory =
    new ProjectProjectionFactory {

      override def bootstrap: IO[Unit] =
        config.indices.parUnorderedTraverse { configuredIndex =>
          val prefixedIndex = configuredIndex.prefixedIndex(config.prefix)
          client.createIndex(prefixedIndex, configuredIndex.indexDef)
        }.void

      override def name(project: ProjectRef): String =
        configuredIndexingProjection(project)

      override def onInit(project: ProjectRef): IO[Unit] = IO.unit

      override def compile(project: ProjectRef): IO[CompiledProjection] = {
        val configuredTypes = config.indices.toList.flatMap(_.types).toSet
        IO.fromEither(
          CompiledProjection.compile(
            configuredIndexingProjectionMetadata(project),
            ExecutionStrategy.PersistentSingleNode,
            Source(annotatedSourceStream.continuous(project, _)),
            NonEmptyChain.one(new AnnotatedSourceToConfiguredDocument(configuredTypes)),
            ConfiguredElasticSink(client, config, batch, Refresh.False)
          )
        )
      }
    }

}
