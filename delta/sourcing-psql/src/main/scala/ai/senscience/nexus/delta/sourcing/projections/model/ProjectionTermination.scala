package ai.senscience.nexus.delta.sourcing.projections.model

import ai.senscience.nexus.delta.sourcing.stream.ProjectionMetadata

import java.time.Instant

/**
  * A persisted record of a projection's terminal lifecycle event.
  *
  * Repeated occurrences of the same logical event (same name, same event type, same error class) collapse into a single
  * row whose [[occurrences]] counter is incremented and whose [[lastOccurrence]] timestamp is refreshed.
  */
sealed trait ProjectionTermination extends Product with Serializable {
  def metadata: ProjectionMetadata
  def firstOccurrence: Instant
  def lastOccurrence: Instant
  def occurrences: Long
}

object ProjectionTermination {

  /**
    * The projection reached a natural terminal state (end-of-stream or passivation).
    */
  final case class Completed(
      metadata: ProjectionMetadata,
      firstOccurrence: Instant,
      lastOccurrence: Instant,
      occurrences: Long
  ) extends ProjectionTermination

  /**
    * The projection terminated with an error. Distinct error classes for the same projection produce distinct rows;
    * repeated occurrences with the same class collapse onto one.
    */
  final case class Failed(
      metadata: ProjectionMetadata,
      errorClass: String,
      errorMessage: String,
      firstOccurrence: Instant,
      lastOccurrence: Instant,
      occurrences: Long
  ) extends ProjectionTermination
}
