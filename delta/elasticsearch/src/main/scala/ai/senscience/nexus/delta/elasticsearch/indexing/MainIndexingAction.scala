package ai.senscience.nexus.delta.elasticsearch.indexing

import ai.senscience.nexus.delta.elasticsearch.client.{ElasticSearchClient, Refresh}
import ai.senscience.nexus.delta.elasticsearch.config.MainIndexConfig
import ai.senscience.nexus.delta.elasticsearch.indexing.MainIndexingCoordinator.mainIndexingPipeline
import ai.senscience.nexus.delta.sdk.indexing.MainDocument
import ai.senscience.nexus.delta.sdk.indexing.{MainDocumentEncoder, SyncIndexingAction}
import ai.senscience.nexus.delta.sdk.indexing.sync.{SyncIndexingOutcome, SyncIndexingRunner}
import ai.senscience.nexus.delta.sdk.model.ResourceF
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sourcing.model.{EntityType, ProjectRef}
import ai.senscience.nexus.delta.sourcing.stream.*
import ai.senscience.nexus.delta.sourcing.stream.Operation.Sink
import ai.senscience.nexus.delta.sourcing.stream.config.BatchConfig
import cats.effect.IO
import fs2.Stream
import org.typelevel.otel4s.trace.Tracer

import scala.concurrent.duration.FiniteDuration

final class MainIndexingAction(encoder: MainDocumentEncoder.Aggregate, sink: Sink, timeout: FiniteDuration)(using
    Tracer[IO]
) extends SyncIndexingAction {

  def apply[A](entityType: EntityType)(project: ProjectRef, res: ResourceF[A]): IO[SyncIndexingOutcome] =
    SyncIndexingAction
      .evalMapToElem(entityType)(project, res, encoder.fromResource(entityType)(_))
      .flatMap { elem =>
        SyncIndexingRunner(Stream.fromEither[IO](compile(project, elem)), timeout)
      }
      .surround("main-sync-index")

  private def compile(project: ProjectRef, elem: Elem[MainDocument]) =
    CompiledProjection.compile(
      mainIndexingProjectionMetadata(project),
      ExecutionStrategy.TransientSingleNode,
      Source(_ => Stream(elem)),
      mainIndexingPipeline,
      sink
    )
}

object MainIndexingAction {
  def apply(
      encoder: MainDocumentEncoder.Aggregate,
      client: ElasticSearchClient,
      config: MainIndexConfig,
      timeout: FiniteDuration,
      syncIndexingRefresh: Refresh
  )(using Tracer[IO]): MainIndexingAction = {
    val batchConfig = BatchConfig.individual
    new MainIndexingAction(
      encoder,
      ElasticSearchSink.mainIndexing(client, batchConfig, config.index, syncIndexingRefresh),
      timeout
    )
  }
}
