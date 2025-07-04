package ai.senscience.nexus.delta.sdk.views

import ai.senscience.nexus.delta.sdk.permissions.model.Permission

/**
  * Describes a view independently of its backend for querying purposes
  */
sealed trait View extends Product with Serializable

object View {

  /**
    * A view that contains data accessible for users with the given permission
    */
  final case class IndexingView(ref: ViewRef, index: String, permission: Permission) extends View

  /**
    * A view that does not contain data by itself but points to other views that does
    */
  final case class AggregateView(views: List[IndexingView]) extends View {

    def ++(view: Option[IndexingView]): AggregateView = AggregateView(views ++ view)

  }

  object AggregateView {

    val empty = AggregateView(List.empty)

  }

}
