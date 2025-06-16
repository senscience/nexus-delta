package ai.senscience.nexus.delta.plugins.blazegraph.supervision

import ai.senscience.nexus.delta.sdk.views.ViewRef
import cats.effect.IO

trait ViewByNamespace {
  def get: IO[Map[String, ViewRef]]
}
