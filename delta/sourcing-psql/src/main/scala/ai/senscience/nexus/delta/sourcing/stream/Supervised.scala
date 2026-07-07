package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.sourcing.stream.Supervised.Control
import cats.effect.IO
import fs2.concurrent.SignallingRef

final case class Supervised(
    metadata: ProjectionMetadata,
    executionStrategy: ExecutionStrategy,
    restarts: Int,
    task: IO[Control],
    control: Control
) {
  def description: IO[SupervisedDescription] =
    for {
      status   <- control.status.get
      progress <- control.progress
    } yield SupervisedDescription(
      metadata,
      executionStrategy,
      restarts,
      status,
      progress
    )
}

object Supervised {
  final case class Control(status: SignallingRef[IO, ExecutionStatus], progress: IO[ProjectionProgress], stop: IO[Unit])
}
