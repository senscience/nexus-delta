package ai.senscience.nexus.delta.sdk.indexing

import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.stream.CompiledProjection
import cats.effect.IO

trait ProjectProjectionFactory {

  def bootstrap: IO[Unit]

  def name(project: ProjectRef): String

  def onInit(project: ProjectRef): IO[Unit]

  def compile(project: ProjectRef): IO[CompiledProjection]
}
