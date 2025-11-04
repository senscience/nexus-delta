package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.sourcing.stream.config.ProjectionConfig.ClusterConfig
import io.circe.{Encoder, Json}

/**
  * Determines how projections should be executed, namely if the current node must run this projection and if offsets
  * should be persisted which is available only if a projection is run on a single node.
  */
enum ExecutionStrategy {

  /**
    * True if the projection must run on all the nodes or if the hash of the projection name modulo number of nodes
    * matches the current node.
    *
    * @param name
    *   the name of the projection
    *
    * @param cluster
    *   the cluster configuration
    */
  def shouldRun(name: String, cluster: ClusterConfig): Boolean = this match {
    case EveryNode => true
    case _         => Math.abs(name.hashCode) % cluster.size == cluster.nodeIndex
  }

  /**
    * Strategy for projections that must run on a single node without persisting the offset.
    */
  case TransientSingleNode

  /**
    * Strategy for projections that must run on a single node persisting the offset.
    */
  case PersistentSingleNode

  /**
    * Strategy for projections that must run on all the nodes, useful for populating caches.
    */
  case EveryNode
}

object ExecutionStrategy {

  given Encoder[ExecutionStrategy] =
    Encoder.instance[ExecutionStrategy] {
      case PersistentSingleNode => Json.fromString("PersistentSingleNode")
      case TransientSingleNode  => Json.fromString("TransientSingleNode")
      case EveryNode            => Json.fromString("EveryNode")
    }
}
