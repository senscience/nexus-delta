package ai.senscience.nexus.delta.sdk.syntax

import ai.senscience.nexus.delta.kernel.error.FormatError
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sdk.jsonld.{IriDecoder, IriEncoder}
import ai.senscience.nexus.delta.sdk.model.BaseUri

trait IriEncodingSyntax {
  implicit final def toIriSyntax[A](value: A): ToIriOpts[A] =
    new ToIriOpts(value)

  implicit final def fromIriSyntax(iri: Iri): FromIriOpts =
    new FromIriOpts(iri)
}

/**
  * Provide extension methods for any A with an [[IriDecoder]]
  *
  * @param value
  *   the value
  */
final class ToIriOpts[A](private val value: A) extends AnyVal {

  /**
    * Encode the value of type [[A]] to an Iri
    */
  def asIri(using base: BaseUri, iriEncoder: IriEncoder[A]): Iri = iriEncoder(value)
}

/**
  * Provide extension methods for iri to parse it into a value of type ``A``
  *
  * @param iri
  *   the iri
  */
final class FromIriOpts(private val iri: Iri) extends AnyVal {

  /**
    * Attempts to decode the Iri into a value of type [[A]]
    */
  def as[A](using base: BaseUri, iriDecoder: IriDecoder[A]): Either[FormatError, A] = iriDecoder(iri)
}
