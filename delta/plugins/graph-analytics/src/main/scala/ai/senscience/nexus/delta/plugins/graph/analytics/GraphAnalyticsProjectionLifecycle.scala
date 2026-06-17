package ai.senscience.nexus.delta.plugins.graph.analytics

import ai.senscience.nexus.delta.elasticsearch.client.ElasticSearchClient
import ai.senscience.nexus.delta.plugins.graph.analytics.GraphAnalytics.{index, projectionName}
import ai.senscience.nexus.delta.plugins.graph.analytics.config.GraphAnalyticsConfig
import ai.senscience.nexus.delta.plugins.graph.analytics.indexing.*
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sdk.indexing.ProjectProjectionLifecycle
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.stream.{CompiledProjection, ExecutionStrategy, ProjectionMetadata, Source}
import cats.effect.IO
import org.typelevel.otel4s.trace.Tracer

object GraphAnalyticsProjectionLifecycle {

  final val id = nxv + "graph-analytics"

  val graphAnalyticsModule: String = "ga"

  private def analyticsMetadata(project: ProjectRef) = ProjectionMetadata(
    graphAnalyticsModule,
    projectionName(project),
    Some(project),
    Some(id)
  )

  def apply(
      analyticsStream: GraphAnalyticsStream,
      client: ElasticSearchClient,
      config: GraphAnalyticsConfig
  )(using Tracer[IO]): Option[ProjectProjectionLifecycle] =
    Option.when(config.indexingEnabled) {
      new ProjectProjectionLifecycle {

        override def module: String = graphAnalyticsModule

        override def bootstrap: IO[Unit] = scriptContent.flatMap { script =>
          client.createScript(updateRelationshipsScriptId, script)
        }

        override def name(project: ProjectRef): String = projectionName(project)

        override def onInit(project: ProjectRef): IO[Unit] =
          graphAnalyticsIndexDef.flatMap { indexDef =>
            client.createIndex(index(config.prefix, project), indexDef)
          }.void

        override def compile(project: ProjectRef): IO[CompiledProjection] =
          IO.fromEither(
            CompiledProjection.compile(
              analyticsMetadata(project),
              ExecutionStrategy.PersistentSingleNode,
              Source(analyticsStream(project, _)),
              sink(project)
            )
          )

        private def sink(project: ProjectRef) =
          new GraphAnalyticsSink(
            client,
            config.batch,
            index(config.prefix, project)
          )
      }
    }

}
