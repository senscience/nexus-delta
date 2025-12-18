package ai.senscience.nexus.delta.elasticsearch.indexing

import ai.senscience.nexus.delta.sourcing.stream.Elem

sealed trait ElemRoutingScheme {
  def apply[A](elem: Elem[A]): Option[String]
}

object ElemRoutingScheme {

  case object Never extends ElemRoutingScheme {
    override def apply[A](elem: Elem[A]): Option[String] = None
  }

  case object ByProject extends ElemRoutingScheme {
    override def apply[A](elem: Elem[A]): Option[String] = Some(elem.project.toString)
  }
}
