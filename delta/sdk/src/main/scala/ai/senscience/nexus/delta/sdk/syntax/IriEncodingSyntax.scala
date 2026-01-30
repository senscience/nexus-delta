package ai.senscience.nexus.delta.sdk.syntax

import ai.senscience.nexus.delta.kernel.error.FormatError
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sdk.jsonld.{IriDecoder, IriEncoder}
import ai.senscience.nexus.delta.sdk.model.BaseUri

trait IriEncodingSyntax {

  extension [A](value: A) {

    /**
      * Encode the value of type [[A]] to an Iri
      */
    def asIri(using base: BaseUri, iriEncoder: IriEncoder[A]): Iri = iriEncoder(value)
  }

  extension (iri: Iri) {

    /**
      * Attempts to decode the Iri into a value of type [[A]]
      */
    def as[A](using base: BaseUri, iriDecoder: IriDecoder[A]): Either[FormatError, A] = iriDecoder(iri)
  }
}
