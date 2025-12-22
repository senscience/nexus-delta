package ai.senscience.nexus.delta.elasticsearch.indexing

import ai.senscience.nexus.delta.elasticsearch.client.{ElasticSearchClient, Refresh}
import ai.senscience.nexus.delta.elasticsearch.main.MainIndexDef
import ai.senscience.nexus.delta.sdk.indexing.ProjectProjectionFactory
import ai.senscience.nexus.delta.sdk.stream.MainDocumentStream
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.stream.config.BatchConfig
import ai.senscience.nexus.delta.sourcing.stream.{CompiledProjection, ExecutionStrategy, Operation, Source}
import cats.data.NonEmptyChain
import cats.effect.IO
import org.typelevel.otel4s.trace.Tracer

object MainIndexingProjectionFactory {

  val mainIndexingPipeline: NonEmptyChain[Operation] = NonEmptyChain.one(MainDocumentToJson)

  def apply(
      mainDocumentStream: MainDocumentStream,
      client: ElasticSearchClient,
      mainIndex: MainIndexDef,
      batch: BatchConfig,
      indexingEnabled: Boolean
  )(using Tracer[IO]): Option[ProjectProjectionFactory] =
    Option.when(indexingEnabled) {
      new ProjectProjectionFactory {
        private val targetIndex          = mainIndex.name
        override def bootstrap: IO[Unit] =
          client.createIndex(targetIndex, mainIndex.indexDef).void

        override def name(project: ProjectRef): String =
          mainIndexingProjection(project)

        override def onInit(project: ProjectRef): IO[Unit] = IO.unit

        override def compile(project: ProjectRef): IO[CompiledProjection] =
          IO.fromEither(
            CompiledProjection.compile(
              mainIndexingProjectionMetadata(project),
              ExecutionStrategy.PersistentSingleNode,
              Source(mainDocumentStream.continuous(project, _)),
              mainIndexingPipeline,
              ElasticSearchSink.mainIndexing(client, batch, targetIndex, Refresh.False)
            )
          )
      }
    }

}
