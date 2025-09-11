package ai.senscience.nexus.delta.sdk

import org.apache.pekko.http.scaladsl.server.Route

/**
  * A [[Route]] that has a ''priority''.
  *
  * @param priority
  *   the priority of this route
  * @param route
  *   the route
  */
final case class PriorityRoute(priority: Int, route: Route, requiresStrictEntity: Boolean)

object PriorityRoute {
  implicit val priorityRouteOrdering: Ordering[PriorityRoute] = Ordering.by(_.priority)
}
