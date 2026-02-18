package ai.senscience.nexus.delta.plugins.blazegraph.indexing

import ai.senscience.nexus.delta.kernel.{Logger, RetryStrategyConfig}
import ai.senscience.nexus.delta.plugins.blazegraph.client.SparqlClient
import ai.senscience.nexus.delta.plugins.blazegraph.indexing.IndexingViewDef.ActiveViewDef
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.stream.GraphResourceStream
import ai.senscience.nexus.delta.sourcing.stream.config.BatchConfig
import ai.senscience.nexus.delta.sourcing.stream.{CompiledProjection, PipeChainCompiler}
import cats.effect.IO
import org.typelevel.otel4s.trace.Tracer

trait SparqlProjectionLifeCycle {

  def compile(view: ActiveViewDef): IO[CompiledProjection]

  def init(view: ActiveViewDef): IO[Unit]

  def destroy(view: ActiveViewDef): IO[Unit]

}

object SparqlProjectionLifeCycle {

  private val logger = Logger[SparqlProjectionLifeCycle]

  def apply(
      graphStream: GraphResourceStream,
      pipeChainCompiler: PipeChainCompiler,
      client: SparqlClient,
      retryStrategy: RetryStrategyConfig,
      batchConfig: BatchConfig
  )(using BaseUri, Tracer[IO]): SparqlProjectionLifeCycle = new SparqlProjectionLifeCycle {

    override def compile(view: ActiveViewDef): IO[CompiledProjection] =
      IndexingViewDef.compile(
        view,
        pipeChainCompiler,
        graphStream.continuous(view.ref.project, view.selectFilter, _),
        sink(view)
      )

    override def init(view: ActiveViewDef): IO[Unit] =
        client.createNamespace(view.namespace).void.onError { case e =>
          logger.error(e)(s"Namespace for view '${view.ref}' could not be created.")
        }

    private def sink(view: ActiveViewDef) = SparqlSink(client, retryStrategy, batchConfig, view.namespace)

    override def destroy(view: ActiveViewDef): IO[Unit] = client.deleteNamespace(view.namespace).void
  }

}
