package ai.senscience.nexus.delta.sourcing.stream

import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder

final case class SupervisedDescription(
    metadata: ProjectionMetadata,
    executionStrategy: ExecutionStrategy,
    restarts: Int,
    status: ExecutionStatus,
    progress: ProjectionProgress
)

object SupervisedDescription {
  given Encoder[SupervisedDescription] = deriveEncoder
}
