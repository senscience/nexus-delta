package ai.senscience.nexus.delta.rdf.jsonld.decoder.configuration

import ai.senscience.nexus.delta.rdf.jsonld.decoder.{Configuration, JsonLdDecoder, MagnoliaJsonLdDecoder}

import scala.deriving.*

object semiauto {

  inline def deriveConfigJsonLdDecoder[A](using Configuration, Mirror.Of[A]): JsonLdDecoder[A] =
    MagnoliaJsonLdDecoder.derived[A]
}
