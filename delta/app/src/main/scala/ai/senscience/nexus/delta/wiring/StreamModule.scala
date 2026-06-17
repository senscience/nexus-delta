package ai.senscience.nexus.delta.wiring

import ai.senscience.nexus.delta.config.BuildInfo
import ai.senscience.nexus.delta.sdk.ResourceShifts
import ai.senscience.nexus.delta.sdk.stream.{AnnotatedSourceStream, GraphResourceStream}
import ai.senscience.nexus.delta.sourcing.config.ElemQueryConfig
import ai.senscience.nexus.delta.sourcing.otel.ProjectionMetrics
import ai.senscience.nexus.delta.sourcing.projections.*
import ai.senscience.nexus.delta.sourcing.query.{ElemStreaming, EntityTypeFilter}
import ai.senscience.nexus.delta.sourcing.model.EntityType
import ai.senscience.nexus.delta.sourcing.stream.*
import ai.senscience.nexus.delta.sourcing.stream.PurgeProjectionCoordinator.PurgeProjection
import ai.senscience.nexus.delta.sourcing.stream.config.{ProjectLastUpdateConfig, ProjectionConfig}
import ai.senscience.nexus.delta.sourcing.stream.pipes.defaultPipes
import ai.senscience.nexus.delta.sourcing.tombstone.StateTombstoneStore
import ai.senscience.nexus.delta.sourcing.{DeleteExpired, PurgeElemFailures, Transactors}
import cats.effect.{Clock, IO, Sync}
import izumi.distage.model.definition.{Id, ModuleDef}
import org.typelevel.otel4s.oteljava.OtelJava

/**
  * Indexing specific wiring.
  */
object StreamModule extends ModuleDef {
  addImplicit[Sync[IO]]

  make[EntityTypeFilter].from { (entityTypes: Set[EntityType] @Id("indexing-types")) =>
    EntityTypeFilter.include(entityTypes)
  }

  make[ElemStreaming].from {
    (
        xas: Transactors,
        entityTypeFilter: EntityTypeFilter,
        queryConfig: ElemQueryConfig,
        projectActivity: ProjectActivity
    ) =>
      new ElemStreaming(xas, entityTypeFilter, queryConfig, projectActivity)
  }

  make[GraphResourceStream].from { (elemStreaming: ElemStreaming, shifts: ResourceShifts) =>
    GraphResourceStream(elemStreaming, shifts)
  }

  make[AnnotatedSourceStream].from { (elemStreaming: ElemStreaming, shifts: ResourceShifts) =>
    AnnotatedSourceStream(elemStreaming, shifts)
  }

  many[PipeDef].addSet(defaultPipes)

  make[PipeChainCompiler].from { (pipes: Set[PipeDef]) =>
    PipeChainCompiler(pipes)
  }

  make[Projections].from {
    (xas: Transactors, entityTypeFilter: EntityTypeFilter, cfg: ProjectionConfig, clock: Clock[IO]) =>
      Projections(xas, entityTypeFilter, cfg.query, clock)
  }

  make[ProjectionErrors].from { (xas: Transactors, clock: Clock[IO], cfg: ProjectionConfig) =>
    ProjectionErrors(xas, cfg.query, clock)
  }

  make[ProjectionTerminalStore].from { (xas: Transactors, cfg: ProjectionConfig) =>
    ProjectionTerminalStore(xas, cfg.query)
  }

  make[ProjectionMetrics].fromEffect { (otel: OtelJava[IO]) =>
    ProjectionMetrics(BuildInfo.version)(using otel.meterProvider)
  }

  make[ProjectionOutcomeListener].fromEffect {
    (terminalLog: ProjectionTerminalStore, clock: Clock[IO], metrics: ProjectionMetrics) =>
      ProjectionOutcomeListener(terminalLog, clock, metrics)
  }

  make[Supervisor].fromResource {
    (
        projections: Projections,
        projectionErrors: ProjectionErrors,
        listener: ProjectionOutcomeListener,
        cfg: ProjectionConfig,
        metrics: ProjectionMetrics
    ) =>
      Supervisor(projections, projectionErrors, listener, cfg, metrics)
  }

  make[ProjectionsRestartScheduler].from { (projections: Projections) =>
    ProjectionsRestartScheduler(projections)
  }

  make[ProjectLastUpdateStore].from { (xas: Transactors) => ProjectLastUpdateStore(xas) }
  make[ProjectLastUpdateStream].from { (xas: Transactors, config: ProjectLastUpdateConfig) =>
    ProjectLastUpdateStream(xas, config.query)
  }

  make[ProjectLastUpdateWrites].fromEffect {
    (
        supervisor: Supervisor,
        store: ProjectLastUpdateStore,
        xas: Transactors,
        config: ProjectLastUpdateConfig
    ) =>
      ProjectLastUpdateWrites(supervisor, store, xas, config.batch)
  }

  make[ProjectionActivations].fromEffect { (metrics: ProjectionMetrics) =>
    ProjectionActivations(metrics)
  }

  make[ProjectActivity].fromEffect {
    (
        supervisor: Supervisor,
        stream: ProjectLastUpdateStream,
        clock: Clock[IO],
        config: ProjectLastUpdateConfig,
        activations: ProjectionActivations,
        otel: OtelJava[IO]
    ) =>
      otel.meterProvider
        .meter("ai.senscience.nexus.delta.projects")
        .withVersion(BuildInfo.version)
        .get
        .flatMap { meter =>
          ProjectActivity(supervisor, stream, clock, config.inactiveInterval, activations)(using meter)
        }
  }

  make[WatchRestarts].fromEffect {
    (supervisor: Supervisor, projections: Projections, activations: ProjectionActivations) =>
      WatchRestarts(supervisor, projections, activations)
  }

  make[PurgeProjectionCoordinator].fromEffect {
    (supervisor: Supervisor, clock: Clock[IO], projections: Set[PurgeProjection]) =>
      PurgeProjectionCoordinator(supervisor, clock, projections)
  }

  many[PurgeProjection].add { (config: ProjectionConfig, xas: Transactors) =>
    DeleteExpired(config.deleteExpiredEvery, xas)
  }

  many[PurgeProjection].add { (config: ProjectionConfig, xas: Transactors) =>
    PurgeElemFailures(config.failedElemPurge, xas)
  }

  many[PurgeProjection].add { (config: ProjectionConfig, xas: Transactors) =>
    StateTombstoneStore.deleteExpired(config.tombstonePurge, xas)
  }
}
