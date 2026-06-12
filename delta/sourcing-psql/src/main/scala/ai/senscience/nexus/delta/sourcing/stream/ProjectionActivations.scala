package ai.senscience.nexus.delta.sourcing.stream

import cats.effect.IO
import fs2.Stream
import fs2.concurrent.Topic

/**
  * Broadcasts [[ProjectionActivation]] events. Producers (the project-activity tracker, the restart watcher) publish
  * activations; consumers (the view coordinators) subscribe via [[events]] to (re)start the matching projections.
  */
trait ProjectionActivations {

  /**
    * Publishes an activation event.
    */
  def publish(activation: ProjectionActivation): IO[Unit]

  /**
    * Stream of activation events. Each call creates an independent subscription so multiple consumers may observe the
    * same events.
    */
  def events: Stream[IO, ProjectionActivation]
}

object ProjectionActivations {

  /**
    * A no-op that drops all activations and exposes an empty stream. Intended for tests where the activation hub is not
    * running.
    */
  val noop: ProjectionActivations = new ProjectionActivations {
    override def publish(activation: ProjectionActivation): IO[Unit] = IO.unit
    override def events: Stream[IO, ProjectionActivation]            = Stream.empty
  }

  /**
    * Constructs a [[ProjectionActivations]] backed by an unbounded FS2 topic.
    */
  def apply(): IO[ProjectionActivations] =
    Topic[IO, ProjectionActivation].map { topic =>
      new ProjectionActivations {
        override def publish(activation: ProjectionActivation): IO[Unit] = topic.publish1(activation).void
        override def events: Stream[IO, ProjectionActivation]            = topic.subscribe(Int.MaxValue)
      }
    }
}
