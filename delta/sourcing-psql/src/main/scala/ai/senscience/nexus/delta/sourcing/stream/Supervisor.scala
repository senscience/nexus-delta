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
import cats.effect.std.Semaphore
import cats.syntax.all.*
import fs2.Stream

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
    * Goes through the running projections, attempt to restart those in error and remove the completed ones
    */
  def check: Stream[IO, Unit]

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
    def init: IO[Supervisor] =
      for {
        _                 <- log.info("Starting Delta supervisor")
        semaphore         <- Semaphore[IO](1L)
        supervisorStorage <- SupervisorStorage()
        supervisor         =
          new Impl(projections, projectionErrors, cfg, semaphore, supervisorStorage, metrics)
        _                 <- WatchRestarts(supervisor, projections)
        _                 <- log.info("Delta supervisor is up")
      } yield supervisor

    Resource.make[IO, Supervisor](init)(_.stop)
  }

  private def createRetryStrategy(cfg: ProjectionConfig, metadata: ProjectionMetadata, action: String) =
    RetryStrategy.retryOnNonFatal(
      cfg.retry,
      log,
      s"$action projection '${metadata.fullName}''"
    )

  private def restartProjection(supervised: Supervised, supervisorStorage: SupervisorStorage): IO[Unit] = {
    val metadata = supervised.metadata
    supervised.task.flatMap { control =>
      supervisorStorage.update(metadata.name, _.copy(restarts = supervised.restarts + 1, control = control))
    }
  }

  private class Impl(
      projections: Projections,
      projectionErrors: ProjectionErrors,
      cfg: ProjectionConfig,
      semaphore: Semaphore[IO],
      supervisorStorage: SupervisorStorage,
      metrics: ProjectionMetrics
  ) extends Supervisor {

    // To persist progress and errors
    private given BatchConfig = cfg.batch

    override def run(projection: CompiledProjection, init: IO[Unit]): IO[Option[ExecutionStatus]] = {
      val metadata = projection.metadata
      semaphore.permit.surround {
        for {
          supervised <- supervisorStorage.get(metadata.name)
          _          <- supervised.traverse { s =>
                          // if a projection with the same name already exists remove from the map and stop it, it will
                          // be re-created
                          log.info(s"Stopping existing projection '${metadata.fullName}'") >>
                            supervisorStorage.delete(metadata.name) >> s.control.stop
                        }
          supervised <- startSupervised(projection, init)
          status     <- supervised.traverse(_.control.status)
        } yield status
      }
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
          _         <- supervisorStorage.add(metadata.name, supervised)
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
            projectionErrors.saveFailedElems(projection.metadata, _)
          )
        case TransientSingleNode | EveryNode =>
          Projection(
            projection,
            init,
            IO.none,
            metrics.recordProgress(projection.metadata, _),
            projectionErrors.saveFailedElems(projection.metadata, _)
          )
      }

    override def restart(name: String, offset: Offset): IO[Option[ExecutionStatus]] =
      semaphore.permit.surround {
        for {
          supervised <- supervisorStorage.get(name)
          status     <- supervised.flatTraverse { s =>
                          val metadata = s.metadata
                          if !s.executionStrategy.shouldRun(name, cfg.cluster) then
                            log.info(s"'${metadata.fullName}' is ignored. Skipping restart...").as(None)
                          else {
                            for {
                              _      <- log.info(s"Restarting '${metadata.fullName}' at offset ${offset.value}...")
                              _      <- stopProjection(s)
                              _      <- IO.whenA(s.executionStrategy == PersistentSingleNode)(
                                          projections.reset(metadata.name, offset)
                                        )
                              _      <- Supervisor.restartProjection(s, supervisorStorage)
                              status <- s.control.status
                            } yield Some(status)
                          }
                        }
        } yield status
      }

    override def destroy(name: String, onDestroy: IO[Unit]): IO[Option[ExecutionStatus]] = {
      semaphore.permit.surround {
        for {
          supervised <- supervisorStorage.get(name)
          status     <- supervised.flatTraverse { s =>
                          val metadata      = s.metadata
                          val retryStrategy = createRetryStrategy(cfg, metadata, "destroying")
                          if !s.executionStrategy.shouldRun(name, cfg.cluster) then
                            log.info(s"'${metadata.fullName}' is ignored. Skipping...").as(None)
                          else {
                            for {
                              _      <- log.info(s"Destroying '${metadata.fullName}'...")
                              _      <- stopProjection(s)
                              _      <- IO.whenA(s.executionStrategy == PersistentSingleNode)(projections.delete(name))
                              _      <- projectionErrors.deleteEntriesForProjection(name)
                              _      <- onDestroy
                                          .retry(retryStrategy)
                                          .handleError(_ => ())
                              status <- s.control.status
                                          .iterateUntil(e => e == ExecutionStatus.Completed || e == ExecutionStatus.Stopped)
                                          .timeout(3.seconds)
                                          .recover { case _: TimeoutException =>
                                            ExecutionStatus.Stopped
                                          }
                            } yield Some(status)
                          }
                        }
          _          <- supervisorStorage.delete(name)
        } yield status
      }
    }

    private def stopProjection(s: Supervised) =
      s.control.stop.handleErrorWith { e =>
        log.error(e)(s"'${s.metadata.fullName}' encountered an error during shutdown.")
      }

    override def describe(name: String): IO[Option[SupervisedDescription]] =
      supervisorStorage.get(name).flatMap { _.traverse(_.description) }

    override def getRunningProjections: IO[List[SupervisedDescription]] =
      supervisorStorage.values.evalMap(_.description).compile.toList

    def check: Stream[IO, Unit] =
      supervisorStorage.values.evalMap { supervised =>
        val metadata = supervised.metadata
        supervised.control.status.flatMap {
          case ExecutionStatus.Pending           => IO.unit
          case ExecutionStatus.Running           => IO.unit
          case ExecutionStatus.Completed         => IO.unit
          case ExecutionStatus.Stopped           => IO.unit
          case ExecutionStatus.Failed(throwable) =>
            val retryStrategy = createRetryStrategy(cfg, metadata, "running")
            log.error(throwable)(s"The projection '${metadata.fullName}' failed and will be restarted.") >>
              semaphore.permit
                .surround(restartProjection(supervised, supervisorStorage))
                .retry(retryStrategy)
        }
      }

    private def stopAllProjections =
      semaphore.permit.surround {
        log.info("Stopping all projection(s)...") >>
          supervisorStorage.values.parEvalMapUnbounded(stopProjection).compile.drain
      }.void

    override def stop: IO[Unit] =
      log.info("Stopping supervisor and all its running projections") >>
        stopAllProjections
  }

}
