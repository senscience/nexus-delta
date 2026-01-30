package ai.senscience.nexus.delta.sdk.projects.model

import ai.senscience.nexus.delta.kernel.error.FormatError
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sdk.error.FormatErrors.{IllegalIRIFormatError, IllegalPrefixIRIFormatError}
import ai.senscience.nexus.delta.sdk.syntax.*
import cats.implicits.*
import io.circe.{Decoder, Encoder}

/**
  * An Iri that ends with ''/'' or ''#''
  */
final case class PrefixIri private (value: Iri) extends AnyVal {
  override def toString: String = value.toString
}

object PrefixIri {

  /**
    * Attempts to construct a [[PrefixIri]] from its Iri representation.
    *
    * @param value
    *   the iri
    */
  final def apply(value: Iri): Either[FormatError, PrefixIri] =
    Option.when(value.isPrefixMapping)(new PrefixIri(value)).toRight(IllegalPrefixIRIFormatError(value))

  /**
    * Attempts to construct a [[PrefixIri]] from its string representation.
    *
    * @param value
    *   the string representation of an iri
    */
  final def apply(value: String): Either[FormatError, PrefixIri] =
    value.toIri.leftMap(_ => IllegalIRIFormatError(value)).flatMap(apply)

  /**
    * Construct [[PrefixIri]] without performing any checks.
    *
    * @param value
    *   the iri
    */
  final def unsafe(value: Iri): PrefixIri =
    new PrefixIri(value)

  given Encoder[PrefixIri] = Encoder.encodeString.contramap(_.value.toString)

  given Decoder[PrefixIri] = Decoder.decodeString.emap(s => PrefixIri(s).leftMap(_.getMessage))

}
