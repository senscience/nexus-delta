package ai.senscience.nexus.delta.sdk.model.metrics

import ai.senscience.nexus.delta.sdk.model.metrics.EventMetric.ProjectScopedMetric
import ai.senscience.nexus.delta.sourcing.event.Event
import ai.senscience.nexus.delta.sourcing.event.Event.ScopedEvent
import ai.senscience.nexus.delta.sourcing.model.EntityType
import io.circe.Decoder

/**
  * Typeclass of Events [[E]] that can be encoded into EventMetric [[M]]
  */
sealed trait EventMetricEncoder[E <: Event, M <: EventMetric] {
  protected def databaseDecoder: Decoder[E]

  def entityType: EntityType

  protected def eventToMetric: E => M

  def toMetric: Decoder[M] =
    databaseDecoder.map(eventToMetric)
}

/**
  * Typeclass of ScopedEvents [[E]] that can be encoded into ProjectScopedMetric
  */
trait ScopedEventMetricEncoder[E <: ScopedEvent] extends EventMetricEncoder[E, ProjectScopedMetric]
