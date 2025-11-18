package ai.senscience.nexus.delta.wiring

import ai.senscience.nexus.delta.config.BuildInfo
import ai.senscience.nexus.delta.sdk.ResourceShifts
import ai.senscience.nexus.delta.sdk.stream.GraphResourceStream
import ai.senscience.nexus.delta.sourcing.config.ElemQueryConfig
import ai.senscience.nexus.delta.sourcing.otel.ProjectionMetrics
import ai.senscience.nexus.delta.sourcing.projections.*
import ai.senscience.nexus.delta.sourcing.query.ElemStreaming
import ai.senscience.nexus.delta.sourcing.stream.*
import ai.senscience.nexus.delta.sourcing.stream.PurgeProjectionCoordinator.PurgeProjection
import ai.senscience.nexus.delta.sourcing.stream.config.{ProjectLastUpdateConfig, ProjectionConfig}
import ai.senscience.nexus.delta.sourcing.stream.pipes.defaultPipes
import ai.senscience.nexus.delta.sourcing.tombstone.StateTombstoneStore
import ai.senscience.nexus.delta.sourcing.{DeleteExpired, PurgeElemFailures, Transactors}
import cats.effect.{Clock, IO, Sync}
import izumi.distage.model.definition.ModuleDef
import org.typelevel.otel4s.oteljava.OtelJava

/**
  * Indexing specific wiring.
  */
object StreamModule extends ModuleDef {
  addImplicit[Sync[IO]]

  make[ElemStreaming].from {
    (xas: Transactors, shifts: ResourceShifts, queryConfig: ElemQueryConfig, activitySignals: ProjectActivitySignals) =>
      new ElemStreaming(xas, shifts.entityTypes, queryConfig, activitySignals)
  }

  make[GraphResourceStream].from { (elemStreaming: ElemStreaming, shifts: ResourceShifts) =>
    GraphResourceStream(elemStreaming, shifts)
  }

  many[PipeDef].addSet(defaultPipes)

  make[PipeChainCompiler].from { (pipes: Set[PipeDef]) =>
    PipeChainCompiler(pipes)
  }

  make[Projections].from { (xas: Transactors, shifts: ResourceShifts, cfg: ProjectionConfig, clock: Clock[IO]) =>
    Projections(xas, shifts.entityTypes, cfg.query, clock)
  }

  make[ProjectionErrors].from { (xas: Transactors, clock: Clock[IO], cfg: ProjectionConfig) =>
    ProjectionErrors(xas, cfg.query, clock)
  }

  make[ProjectionMetrics].fromEffect { (otel: OtelJava[IO]) =>
    ProjectionMetrics(BuildInfo.version)(using otel.meterProvider)
  }

  make[Supervisor].fromResource {
    (projections: Projections, projectionErrors: ProjectionErrors, cfg: ProjectionConfig, metrics: ProjectionMetrics) =>
      Supervisor(projections, projectionErrors, cfg, metrics)
  }

  make[SupervisorCheck].fromResource { (supervisor: Supervisor, cfg: ProjectionConfig) =>
    SupervisorCheck(supervisor, cfg.supervisionCheckInterval)
  }

  make[ProjectionsRestartScheduler].from { (projections: Projections) =>
    ProjectionsRestartScheduler(projections)
  }

  make[ProjectLastUpdateStore].from { (xas: Transactors) => ProjectLastUpdateStore(xas) }
  make[ProjectLastUpdateStream].from { (xas: Transactors, config: ProjectLastUpdateConfig) =>
    ProjectLastUpdateStream(xas, config.query)
  }

  make[ProjectLastUpdateWrites].fromEffect {
    (supervisor: Supervisor, store: ProjectLastUpdateStore, xas: Transactors, config: ProjectLastUpdateConfig) =>
      ProjectLastUpdateWrites(supervisor, store, xas, config.batch)
  }

  make[ProjectActivitySignals].fromEffect {
    (
        supervisor: Supervisor,
        stream: ProjectLastUpdateStream,
        clock: Clock[IO],
        config: ProjectLastUpdateConfig,
        otel: OtelJava[IO]
    ) =>
      otel.meterProvider
        .meter("ai.senscience.nexus.delta.projects")
        .withVersion(BuildInfo.version)
        .get
        .flatMap { meter =>
          ProjectActivitySignals(supervisor, stream, clock, config.inactiveInterval)(using meter)
        }
  }

  make[PurgeProjectionCoordinator.type].fromEffect {
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
