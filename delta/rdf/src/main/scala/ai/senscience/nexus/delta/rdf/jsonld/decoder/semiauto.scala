package ai.senscience.nexus.delta.rdf.jsonld.decoder

import scala.deriving.Mirror

object semiauto {

  given Configuration = Configuration.default

  inline def deriveDefaultJsonLdDecoder[A](using Mirror.Of[A]): JsonLdDecoder[A] =
    MagnoliaJsonLdDecoder.derived[A]

}
