package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.sourcing.otel.ProjectionMetrics
import ai.senscience.nexus.delta.sourcing.otel.ProjectionMetrics.TerminationOutcome
import ai.senscience.nexus.delta.sourcing.projections.ProjectionTerminalStore
import ai.senscience.nexus.delta.sourcing.stream.ProjectionOutcomeListener.Outcome
import cats.effect.{Clock, IO}
import fs2.Stream
import fs2.concurrent.Channel

/**
  * Receives terminal-state notifications from a running [[Projection]] and exposes them as a single stream for the
  * supervisor to react to projection failures (for retry) and natural completions (for eviction from supervision).
  */
trait ProjectionOutcomeListener {

  /**
    * Invoked when the projection fiber terminates with an error. Both the runtime stream (used by the supervisor for
    * retry) and the persistent store are updated.
    */
  def onFailure(metadata: ProjectionMetadata, error: Throwable): IO[Unit]

  /**
    * Invoked when the projection fiber terminates by completing its stream (end-of-data or passivation).
    *
    * The runtime stream (used by the supervisor for eviction) is always updated. The persistent store is updated only
    * when `didWork` is `true` — i.e., the projection processed at least one new element during this run. A restart that
    * immediately passivates without progress passes `false` to avoid polluting the store.
    */
  def onCompletion(metadata: ProjectionMetadata, didWork: Boolean): IO[Unit]

  /**
    * Single stream of terminal outcomes (failures and completions) signalled via [[onFailure]] / [[onCompletion]].
    */
  def outcomes: Stream[IO, Outcome]
}

object ProjectionOutcomeListener {

  /**
    * A terminal outcome signalled by a running [[Projection]] fiber.
    */
  sealed trait Outcome extends Product with Serializable {
    def name: String
  }

  object Outcome {

    /**
      * The projection fiber terminated with an error; the supervisor should heal/retry it.
      */
    final case class Failed(name: String) extends Outcome

    /**
      * The projection fiber completed its stream (end-of-data or passivation); the supervisor should evict it.
      */
    final case class Completed(name: String) extends Outcome
  }

  val noop: ProjectionOutcomeListener = new ProjectionOutcomeListener {
    override def onFailure(metadata: ProjectionMetadata, error: Throwable): IO[Unit]    = IO.unit
    override def onCompletion(metadata: ProjectionMetadata, didWork: Boolean): IO[Unit] = IO.unit
    override def outcomes: Stream[IO, Outcome]                                          = Stream.empty
  }

  def apply(
      store: ProjectionTerminalStore,
      clock: Clock[IO],
      metrics: ProjectionMetrics
  ): IO[ProjectionOutcomeListener] =
    Channel.unbounded[IO, Outcome].map { channel =>
      new ProjectionOutcomeListener {
        override def onFailure(metadata: ProjectionMetadata, error: Throwable): IO[Unit] =
          metrics.recordTermination(metadata, TerminationOutcome.Failed) >>
            clock.realTimeInstant.flatMap(store.recordFailure(metadata, error, _)) >>
            channel.send(Outcome.Failed(metadata.name)).void

        override def onCompletion(metadata: ProjectionMetadata, didWork: Boolean): IO[Unit] =
          metrics.recordTermination(metadata, TerminationOutcome.Completed) >>
            IO.whenA(didWork)(clock.realTimeInstant.flatMap(store.recordCompletion(metadata, _))) >>
            channel.send(Outcome.Completed(metadata.name)).void

        override def outcomes: Stream[IO, Outcome] = channel.stream
      }
    }
}
