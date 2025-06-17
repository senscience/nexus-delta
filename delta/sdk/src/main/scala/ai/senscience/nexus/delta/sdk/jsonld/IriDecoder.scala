package ai.senscience.nexus.delta.sdk.jsonld

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ch.epfl.bluebrain.nexus.delta.kernel.error.FormatError
import io.circe.Decoder

/**
  * A type class that provides a way to produce a value of type [[A]] from an [[Iri]] value.
  */
trait IriDecoder[A] {

  /**
    * Decode the given [[Iri]]
    */
  def apply(iri: Iri)(implicit base: BaseUri): Either[FormatError, A]
}

object IriDecoder {

  /**
    * Create a circe decoder for [[A]] when it is encoded as an Iri
    */
  def jsonDecoder[A](implicit base: BaseUri, iriDecoder: IriDecoder[A]): Decoder[A] =
    Decoder[Iri].emap(iri =>
      iriDecoder(iri) match {
        case Right(a)  => Right(a)
        case Left(err) => Left(err.toString)
      }
    )

}
