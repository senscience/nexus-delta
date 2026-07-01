package ai.senscience.nexus.delta.sdk

import ai.senscience.nexus.delta.sdk.directives.RouteClassifier
import org.apache.pekko.http.scaladsl.server.Route

/**
  * A [[Route]] contributed to the server assembly, with its mounting metadata (priority, strict-entity requirement, and
  * an optional tracing classifier).
  *
  * @param priority
  *   the priority of this route
  * @param route
  *   the route
  * @param requiresStrictEntity
  *   whether the route requires a strict entity
  * @param classifier
  *   an optional classifier naming this route's paths for tracing; `None` means the route is not traced
  */
final case class RouteEntry(
    priority: Int,
    route: Route,
    requiresStrictEntity: Boolean,
    classifier: Option[RouteClassifier] = None
)

object RouteEntry {
  given routeEntryOrdering: Ordering[RouteEntry] = Ordering.by(_.priority)

  /** Builds a traced [[RouteEntry]], naming its paths with the given `classifier`. */
  def apply(priority: Int, route: Route, requiresStrictEntity: Boolean, classifier: RouteClassifier): RouteEntry =
    RouteEntry(priority, route, requiresStrictEntity, Some(classifier))
}
