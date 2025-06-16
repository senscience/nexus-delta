package ai.senscience.nexus.delta.plugins.blazegraph.supervision

import ai.senscience.nexus.delta.plugins.blazegraph.BlazegraphViews
import ai.senscience.nexus.delta.plugins.blazegraph.indexing.IndexingViewDef
import ai.senscience.nexus.delta.plugins.blazegraph.indexing.IndexingViewDef.ActiveViewDef
import ai.senscience.nexus.delta.sdk.views.ViewRef
import cats.effect.IO
import fs2.Stream

/**
  * Allows to get a mapping for the active blazegraph views between their namespace and their view reference
  */
object BlazegraphViewByNamespace {

  def apply(blazegraphViews: BlazegraphViews): ViewByNamespace = apply(
    blazegraphViews.currentIndexingViews.map(_.value)
  )

  def apply(stream: Stream[IO, IndexingViewDef]): ViewByNamespace = new ViewByNamespace {
    override def get: IO[Map[String, ViewRef]] = stream
      .fold(Map.empty[String, ViewRef]) {
        case (acc, view: ActiveViewDef) => acc + (view.namespace -> view.ref)
        case (acc, _)                   => acc
      }
      .compile
      .last
      .map(_.getOrElse(Map.empty))
  }

}
