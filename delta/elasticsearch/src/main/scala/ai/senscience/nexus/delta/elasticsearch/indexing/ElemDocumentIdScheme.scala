package ai.senscience.nexus.delta.elasticsearch.indexing

import ai.senscience.nexus.delta.sourcing.stream.Elem

sealed trait ElemDocumentIdScheme {

  def apply[A](elem: Elem[A]): String

}

object ElemDocumentIdScheme {

  object ByEvent extends ElemDocumentIdScheme {
    override def apply[A](elem: Elem[A]): String = s"${elem.project}/${elem.id}:${elem.rev}"
  }

  object ByProject extends ElemDocumentIdScheme {
    override def apply[A](elem: Elem[A]): String = s"${elem.project}_${elem.id}"
  }

  object ById extends ElemDocumentIdScheme {
    override def apply[A](elem: Elem[A]): String = elem.id.toString
  }

}
