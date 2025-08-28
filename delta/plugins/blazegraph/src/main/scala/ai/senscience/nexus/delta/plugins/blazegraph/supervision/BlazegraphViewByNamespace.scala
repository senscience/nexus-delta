package ai.senscience.nexus.delta.plugins.blazegraph.supervision

import ai.senscience.nexus.delta.plugins.blazegraph.indexing.CurrentActiveViews
import ai.senscience.nexus.delta.sdk.views.ViewRef
import cats.effect.IO

/**
  * Allows to get a mapping for the active blazegraph views between their namespace and their view reference
  */
object BlazegraphViewByNamespace {

  def apply(currentViews: CurrentActiveViews): ViewByNamespace = new ViewByNamespace {
    override def get: IO[Map[String, ViewRef]] = currentViews.stream
      .fold(Map.empty[String, ViewRef]) { case (acc, view) =>
        acc + (view.namespace -> view.ref)
      }
      .compile
      .last
      .map(_.getOrElse(Map.empty))
  }

}
