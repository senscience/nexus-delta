package ai.senscience.nexus.delta.rdf.syntax

import ai.senscience.nexus.delta.rdf.IriOrBNode.{BNode, Iri}

trait IriSyntax {

  extension (sc: StringContext) {

    /**
      * Construct an Iri without checking the validity of the format.
      */
    def iri(args: Any*): Iri = Iri.unsafe(sc.s(args*))

    /**
      * Construct a [[BNode]].
      */
    def bnode(args: Any*): BNode = BNode.unsafe(sc.s(args*))
  }

  extension (string: String) {

    /**
      * Attempts to construct an absolute Iri, returning a Left when it does not have the correct Iri format.
      */
    def toIri: Either[String, Iri] = Iri.reference(string)
  }
}
