package ai.senscience.nexus.delta.elasticsearch.metrics

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.sdk.model.metrics.EventMetric.*
import ai.senscience.nexus.delta.sdk.model.metrics.ScopedEventMetricEncoder
import ai.senscience.nexus.delta.sourcing.config.QueryConfig
import ai.senscience.nexus.delta.sourcing.event.EventStreaming
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.projections.Projections
import ai.senscience.nexus.delta.sourcing.stream.*
import ai.senscience.nexus.delta.sourcing.stream.Operation.Sink
import ai.senscience.nexus.delta.sourcing.stream.config.BatchConfig
import ai.senscience.nexus.delta.sourcing.{MultiDecoder, Scope, Transactors}
import cats.effect.IO
import cats.effect.std.Env

trait EventMetricsProjection

object EventMetricsProjection {
  private val logger = Logger[EventMetricsProjection]

  val projectionMetadata: ProjectionMetadata = ProjectionMetadata("system", "event-metrics", None, None)

  // We need a value to return to Distage
  private val dummy = new EventMetricsProjection {}

  /**
    * @param metricEncoders
    *   a set of encoders for all entity
    * @param supervisor
    *   the supervisor which will supervise the projection
    * @param eventMetrics
    *   the eventMetricsModule
    * @param xas
    *   doobie transactors
    * @param batchConfig
    *   Elasticsearch batch config
    * @param queryConfig
    *   query config for fetching scoped events
    * @return
    *   a Task that registers a projection with the supervisor which reads all scoped events and pushes their metrics to
    *   Elasticsearch. Events of implementations of ScopedEvents that do not have an instance of
    *   ScopedEventMetricEncoder are silently ignored.
    */
  def apply(
      metricEncoders: Set[ScopedEventMetricEncoder[?]],
      supervisor: Supervisor,
      projections: Projections,
      eventMetrics: EventMetrics,
      xas: Transactors,
      batchConfig: BatchConfig,
      queryConfig: QueryConfig,
      indexingEnabled: Boolean
  ): IO[EventMetricsProjection] = if indexingEnabled then {
    val allEntityTypes = metricEncoders.map(_.entityType).toList

    given MultiDecoder[ProjectScopedMetric] = MultiDecoder(metricEncoders.map { encoder =>
      encoder.entityType -> encoder.toMetric
    }.toMap)

    // define how to get metrics from a given offset
    val metrics = (offset: Offset) => EventStreaming.fetchScoped(Scope.root, allEntityTypes, offset, queryConfig, xas)

    val sink = new EventMetricsSink(eventMetrics, batchConfig)

    for {
      shouldRestart     <- Env[IO].get("RESET_EVENT_METRICS").map(_.getOrElse("false").toBoolean)
      _                 <- IO.whenA(shouldRestart)(
                             logger.warn("Resetting event metrics as the env RESET_EVENT_METRICS is set...") >>
                               eventMetrics.destroy >>
                               projections.reset(projectionMetadata.name, Offset.start)
                           )
      metricsProjection <- apply(sink, supervisor, metrics, eventMetrics.init)
    } yield metricsProjection

  } else IO.pure(dummy)

  /**
    * Test friendly apply method
    */
  def apply(
      sink: Sink,
      supervisor: Supervisor,
      metrics: Offset => ElemStream[ProjectScopedMetric],
      init: IO[Unit]
  ): IO[EventMetricsProjection] = {

    val source = Source { (offset: Offset) => metrics(offset) }

    val compiledProjection =
      CompiledProjection.compile(
        projectionMetadata,
        ExecutionStrategy.PersistentSingleNode,
        source,
        sink
      )(using ProjectionBackpressure.Noop)

    IO.fromEither(compiledProjection)
      .flatTap(supervisor.run(_, init))
      .as(dummy)
  }
}
