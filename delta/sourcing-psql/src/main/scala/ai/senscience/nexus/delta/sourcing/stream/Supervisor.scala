package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.kernel.syntax.*
import ai.senscience.nexus.delta.kernel.{Logger, RetryStrategy}
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.otel.ProjectionMetrics
import ai.senscience.nexus.delta.sourcing.projections.{ProjectionErrors, ProjectionTerminalStore, Projections}
import ai.senscience.nexus.delta.sourcing.stream.ExecutionStrategy.{EveryNode, PersistentSingleNode, TransientSingleNode}
import ai.senscience.nexus.delta.sourcing.stream.Supervised.Control
import ai.senscience.nexus.delta.sourcing.stream.config.{BatchConfig, ProjectionConfig}
import cats.effect.*
import cats.syntax.all.*

import scala.concurrent.TimeoutException
import scala.concurrent.duration.*

/**
  * Supervises the execution of projections based on a defined [[ExecutionStrategy]] that describes whether projections
  * should be executed on all the nodes or a single node and whether offsets should be persisted.
  *
  * It monitors and restarts automatically projections that have failed.
  *
  * Projections that complete naturally (including via passivation) are evicted from supervision so they do not pile up
  * in memory. Persisted progress and recorded errors are preserved so the projection can be resumed later via [[run]].
  * `describe` returns `None` for evicted projections; callers requiring completion status for completed projections
  * should consult the `Projections` module which holds persisted progress.
  *
  * When the supervisor is stopped, all running projections are also stopped.
  */
trait Supervisor {

  /**
    * Supervises the execution of the provided `projection`. A second call to this method with a projection with the
    * same name will cause the current projection to be stopped and replaced by the new one.
    * @param projection
    *   the projection to supervise
    * @param init
    *   an initialize task to perform before starting
    * @see
    *   [[Supervisor]]
    */
  def run(projection: CompiledProjection, init: IO[Unit]): IO[Option[ExecutionStatus]]

  /**
    * Supervises the execution of the provided `projection`. A second call to this method with a projection with the
    * same name will cause the current projection to be stopped and replaced by the new one.
    * @param projection
    *   the projection to supervise
    * @see
    *   [[Supervisor]]
    */
  def run(projection: CompiledProjection): IO[Option[ExecutionStatus]] = run(projection, IO.unit)

  /**
    * Restart the given projection from the given offset
    */
  def resetForRestart(name: String, offset: Offset): IO[Boolean]

  /**
    * Stops the supervised projection and removes it from supervision. It performs a noop if the projection is not
    * supervised on the current node and the current node would not have owned it (per its
    * [[ExecutionStrategy.shouldRun]]). It executes the provided finalizer after the projection is stopped.
    * @param projection
    *   the projection to destroy
    * @param clear
    *   the task to be executed after the projection is destroyed
    */
  def destroy(projection: CompiledProjection, clear: IO[Unit]): IO[Option[ExecutionStatus]]

  /**
    * Stops the supervised projection and removes it from supervision. See [[destroy(projection,clear)]] for the full
    * behavior.
    * @param projection
    *   the projection to destroy
    */
  def destroy(projection: CompiledProjection): IO[Option[ExecutionStatus]] = destroy(projection, IO.unit)

  /**
    * Returns the status of the projection with the provided `name`, if a projection with such name exists.
    * @param name
    *   the name of the projection
    */
  def describe(name: String): IO[Option[SupervisedDescription]]

  /**
    * Returns the list of all running projections under this supervisor.
    * @return
    *   a list of the currently running projections
    */
  def getRunningProjections: IO[List[SupervisedDescription]]

  /**
    * Stops all running projections without removing them from supervision.
    */
  def stop: IO[Unit]

}

object Supervisor {

  private val log = Logger[Supervisor]

  /**
    * Constructs a new [[Supervisor]] instance using the provided `store` and `cfg`.
    *
    * @param projections
    *   the projections module
    * @param projectionErrors
    *   the projections error module
    * @param cfg
    *   the projection configuration
    */
  def apply(
      projections: Projections,
      projectionErrors: ProjectionErrors,
      terminalLog: ProjectionTerminalStore,
      cfg: ProjectionConfig,
      metrics: ProjectionMetrics,
      clock: Clock[IO]
  ): Resource[IO, Supervisor] = {
    for {
      _                 <- Resource.eval(log.info("Starting Delta supervisor"))
      supervisorStorage <- Resource.eval(SupervisorStorage())
      listener          <- Resource.eval(ProjectionOutcomeListener(terminalLog, clock))
      _                 <- SupervisorCheck(supervisorStorage, listener, cfg)
      supervisor        <-
        Resource.make(IO.pure(new Impl(projections, projectionErrors, cfg, supervisorStorage, listener, metrics)))(
          _.stop
        )
      _                 <- Resource.eval(log.info("Delta supervisor is up"))
    } yield supervisor
  }

  private[stream] def createRetryStrategy(cfg: ProjectionConfig, projectionLabel: String, action: String) =
    RetryStrategy.retryOnNonFatal(
      cfg.retry,
      log,
      s"$action projection '$projectionLabel''"
    )

  private[stream] def restartProjection(supervised: Supervised): IO[Supervised] =
    supervised.task.map { control =>
      supervised.copy(restarts = supervised.restarts + 1, control = control)
    }

  private class Impl(
      projections: Projections,
      projectionErrors: ProjectionErrors,
      cfg: ProjectionConfig,
      supervisorStorage: SupervisorStorage,
      listener: ProjectionOutcomeListener,
      metrics: ProjectionMetrics
  ) extends Supervisor {

    // To persist progress and errors
    private given BatchConfig = cfg.batch

    override def run(projection: CompiledProjection, init: IO[Unit]): IO[Option[ExecutionStatus]] = {
      val metadata = projection.metadata
      supervisorStorage
        .updateWith(metadata.name) {
          case Some(existing) =>
            // if a projection with the same name already exists remove from the map and stop it, it will
            // be re-created
            log.info(s"Stopping existing projection '${metadata.fullName}'") >>
              existing.control.stop >> startSupervised(projection, init)
          case None           => startSupervised(projection, init)
        }
        .flatMap(_.traverse(_.control.status))
    }

    private def startSupervised(projection: CompiledProjection, init: IO[Unit]): IO[Option[Supervised]] = {
      val metadata      = projection.metadata
      val strategy      = projection.executionStrategy
      val retryStrategy = createRetryStrategy(cfg, metadata.fullName, "init")
      if !strategy.shouldRun(metadata.name, cfg.cluster) then
        log.debug(s"Ignoring '${metadata.fullName}' with strategy '$strategy'.").as(None)
      else {
        for {
          _         <- log.info(s"Starting '${metadata.fullName}' with strategy '$strategy'.")
          controlIO  = startProjection(projection).preAllocate(init.retry(retryStrategy)).allocated.map {
                         case (p, release) => Control(p.executionStatus, p.currentProgress, release)
                       }
          control   <- controlIO
          supervised = Supervised(metadata, projection.executionStrategy, 0, controlIO, control)
        } yield Some(supervised)
      }
    }

    private def startProjection(projection: CompiledProjection): Resource[IO, Projection] =
      projection.executionStrategy match {
        case PersistentSingleNode            =>
          def saveProgressWithMetrics(progress: ProjectionProgress) =
            projections.save(projection.metadata, progress) >>
              metrics.recordProgress(projection.metadata, progress)
          Projection(
            projection,
            projections.progress(projection.metadata.name),
            saveProgressWithMetrics,
            projectionErrors.saveFailedElems(projection.metadata, _),
            listener
          )
        case TransientSingleNode | EveryNode =>
          Projection(
            projection,
            IO.none,
            metrics.recordProgress(projection.metadata, _),
            projectionErrors.saveFailedElems(projection.metadata, _),
            listener
          )
      }

    override def resetForRestart(name: String, offset: Offset): IO[Boolean] = {
      val responsible = PersistentSingleNode.shouldRun(name, cfg.cluster)
      supervisorStorage.delete(name)(stopProjection) >>
        IO.whenA(responsible)(projections.reset(name, offset)).as(responsible)
    }

    override def destroy(projection: CompiledProjection, onDestroy: IO[Unit]): IO[Option[ExecutionStatus]] = {
      val metadata      = projection.metadata
      val name          = metadata.name
      val retryStrategy = createRetryStrategy(cfg, metadata.fullName, "destroying")
      val runCleanup    =
        IO.whenA(projection.executionStrategy == PersistentSingleNode)(projections.delete(name)) >>
          projectionErrors.deleteEntriesForProjection(name) >>
          onDestroy.retry(retryStrategy).handleError(_ => ())
      supervisorStorage
        .delete(name) { supervised =>
          log.info(s"Destroying '${metadata.fullName}'...") >>
            stopProjection(supervised) >>
            supervised.control.status
              .iterateUntil(e => e == ExecutionStatus.Completed || e == ExecutionStatus.Stopped)
              .timeout(3.seconds)
              .recover { case _: TimeoutException => ExecutionStatus.Stopped }
        }
        .flatMap { status =>
          if projection.executionStrategy.shouldRun(name, cfg.cluster) then {
            IO.whenA(status.isEmpty)(log.info(s"Destroying unsupervised projection '${metadata.fullName}'...")) >>
              runCleanup.as(status.orElse(Some(ExecutionStatus.Stopped)))
          } else IO.pure(status)
        }
    }

    private def stopProjection(supervised: Supervised) =
      supervised.control.stop.handleErrorWith { e =>
        log.error(e)(s"'${supervised.metadata.fullName}' encountered an error during shutdown.")
      }

    override def describe(name: String): IO[Option[SupervisedDescription]] =
      supervisorStorage.get(name).flatMap { _.traverse(_.description) }

    override def getRunningProjections: IO[List[SupervisedDescription]] =
      supervisorStorage.values.evalMap(_.description).compile.toList

    private def stopAllProjections =
      log.info("Stopping all projection(s)...") >>
        supervisorStorage.values.parEvalMapUnbounded(stopProjection).compile.drain

    override def stop: IO[Unit] =
      log.info("Stopping supervisor and all its running projections") >>
        stopAllProjections
  }

}
