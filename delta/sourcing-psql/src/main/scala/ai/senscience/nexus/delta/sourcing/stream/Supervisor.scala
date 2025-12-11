package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.kernel.syntax.*
import ai.senscience.nexus.delta.kernel.{Logger, RetryStrategy}
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.otel.ProjectionMetrics
import ai.senscience.nexus.delta.sourcing.projections.{ProjectionErrors, Projections}
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
  * It monitors and restarts automatically projections that have stopped or failed.
  *
  * Projections that completed naturally are not restarted or cleaned up such that the status can be read.
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
    * @param name
    *   the name of the projection
    */
  def restart(name: String, offset: Offset): IO[Option[ExecutionStatus]]

  /**
    * Stops the projection with the provided `name` and removes it from supervision. It performs a noop if the
    * projection does not exist or it is not running on the current node. It executes the provided finalizer after the
    * projection is stopped.
    * @param name
    *   the name of the projection
    * @param clear
    *   the task to be executed after the projection is destroyed
    */
  def destroy(name: String, clear: IO[Unit]): IO[Option[ExecutionStatus]]

  /**
    * Stops the projection with the provided `name` and removes it from supervision. It performs a noop if the
    * projection does not exist or it is not running on the current node. It executes the provided finalizer after the
    * projection is stopped.
    * @param name
    *   the name of the projection
    */
  def destroy(name: String): IO[Option[ExecutionStatus]] = destroy(name, IO.unit)

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
      cfg: ProjectionConfig,
      metrics: ProjectionMetrics
  ): Resource[IO, Supervisor] = {
    for {
      _                 <- Resource.eval(log.info("Starting Delta supervisor"))
      supervisorStorage <- Resource.eval(SupervisorStorage())
      _                 <- SupervisorCheck(supervisorStorage, cfg)
      supervisor        <-
        Resource.make(IO.pure(new Impl(projections, projectionErrors, cfg, supervisorStorage, metrics)))(_.stop)
      _                 <- Resource.eval(WatchRestarts(supervisor, projections))
      _                 <- Resource.eval(log.info("Delta supervisor is up"))
    } yield supervisor
  }

  private[stream] def createRetryStrategy(cfg: ProjectionConfig, metadata: ProjectionMetadata, action: String) =
    RetryStrategy.retryOnNonFatal(
      cfg.retry,
      log,
      s"$action projection '${metadata.fullName}''"
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
      val metadata = projection.metadata
      val strategy = projection.executionStrategy
      if !strategy.shouldRun(metadata.name, cfg.cluster) then
        log.debug(s"Ignoring '${metadata.fullName}' with strategy '$strategy'.").as(None)
      else {
        for {
          _         <- log.info(s"Starting '${metadata.fullName}' with strategy '$strategy'.")
          controlIO  = startProjection(init, projection).map { p =>
                         Control(
                           p.executionStatus,
                           p.currentProgress,
                           p.stop()
                         )
                       }
          control   <- controlIO
          supervised = Supervised(metadata, projection.executionStrategy, 0, controlIO, control)
        } yield Some(supervised)
      }
    }

    private def startProjection(init: IO[Unit], projection: CompiledProjection): IO[Projection] =
      projection.executionStrategy match {
        case PersistentSingleNode            =>
          def saveProgressWithMetrics(progress: ProjectionProgress) =
            projections.save(projection.metadata, progress) >>
              metrics.recordProgress(projection.metadata, progress)
          Projection(
            projection,
            init,
            projections.progress(projection.metadata.name),
            saveProgressWithMetrics,
            projectionErrors.saveFailedElems(projection.metadata, _),
            supervisorStorage.sendFailing
          )
        case TransientSingleNode | EveryNode =>
          Projection(
            projection,
            init,
            IO.none,
            metrics.recordProgress(projection.metadata, _),
            projectionErrors.saveFailedElems(projection.metadata, _),
            supervisorStorage.sendFailing
          )
      }

    override def restart(name: String, offset: Offset): IO[Option[ExecutionStatus]] =
      supervisorStorage
        .update(name) { supervised =>
          val metadata = supervised.metadata
          for {
            _         <- log.info(s"Restarting '${metadata.fullName}' at offset ${offset.value}...")
            _         <- stopProjection(supervised)
            _         <- IO.whenA(supervised.executionStrategy == PersistentSingleNode)(
                           projections.reset(metadata.name, offset)
                         )
            restarted <- Supervisor.restartProjection(supervised)
          } yield restarted
        }
        .flatMap(_.traverse(_.control.status))

    override def destroy(name: String, onDestroy: IO[Unit]): IO[Option[ExecutionStatus]] =
      supervisorStorage.delete(name) { supervised =>
        val metadata      = supervised.metadata
        val retryStrategy = createRetryStrategy(cfg, metadata, "destroying")
        for {
          _      <- log.info(s"Destroying '${metadata.fullName}'...")
          _      <- stopProjection(supervised)
          _      <- IO.whenA(supervised.executionStrategy == PersistentSingleNode)(projections.delete(name))
          _      <- projectionErrors.deleteEntriesForProjection(name)
          _      <- onDestroy
                      .retry(retryStrategy)
                      .handleError(_ => ())
          status <- supervised.control.status
                      .iterateUntil(e => e == ExecutionStatus.Completed || e == ExecutionStatus.Stopped)
                      .timeout(3.seconds)
                      .recover { case _: TimeoutException => ExecutionStatus.Stopped }
        } yield status
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
