package ai.senscience.nexus.delta.plugins.compositeviews.model

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import io.circe.Encoder

/**
  * Enumeration of composite view source types
  */
sealed trait SourceType {

  /**
    * @return
    *   the type id
    */
  def tpe: Iri

  /**
    * @return
    *   the full set of types
    */
  def types: Set[Iri] = Set(tpe, nxv + "CompositeViewSource")

}

object SourceType {

  /**
    * A source for the current project.
    */
  case object ProjectSourceType extends SourceType {
    override val toString: String = "ProjectEventStream"
    override val tpe: Iri         = nxv + toString
  }

  /**
    * A cross project source.
    */
  case object CrossProjectSourceType extends SourceType {
    override val toString: String = "CrossProjectEventStream"
    override val tpe: Iri         = nxv + toString
  }

  /**
    * A remote project source.
    */
  case object RemoteProjectSourceType extends SourceType {
    override val toString: String = "RemoteProjectEventStream"
    override val tpe: Iri         = nxv + toString
  }

  implicit val sourceTypeEncoder: Encoder[SourceType] = Encoder.encodeString.contramap(_.tpe.toString)
}
