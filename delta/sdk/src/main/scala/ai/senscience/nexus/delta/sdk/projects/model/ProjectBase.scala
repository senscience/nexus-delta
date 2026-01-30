package ai.senscience.nexus.delta.sdk.projects.model

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import io.circe.{Decoder, Encoder}

/**
  * Holds the project base Iri
  */
final case class ProjectBase(iri: Iri) extends AnyVal {
  override def toString: String = iri.toString
}

object ProjectBase {

  given Encoder[ProjectBase] = Encoder.encodeString.contramap(_.iri.toString)
  given Decoder[ProjectBase] = Iri.iriDecoder.map(new ProjectBase(_))
}
