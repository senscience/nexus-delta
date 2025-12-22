package ai.senscience.nexus.delta.sdk

import ai.senscience.nexus.delta.sdk.model.ResourceF
import ai.senscience.nexus.delta.sourcing.model.{EntityType, ProjectRef}
import ai.senscience.nexus.delta.sourcing.state.GraphResource
import cats.effect.IO

trait GraphResourceEncoder {

  def encodeResource[A](entityType: EntityType)(project: ProjectRef, resource: ResourceF[A]): IO[GraphResource]

}
