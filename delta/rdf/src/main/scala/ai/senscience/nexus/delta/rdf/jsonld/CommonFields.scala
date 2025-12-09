package ai.senscience.nexus.delta.rdf.jsonld

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary

object CommonFields {

  private def getString(cursor: ExpandedJsonLdCursor, property: Iri, fallback: Iri) =
    cursor
      .downField(property)
      .get[String]
      .orElse(
        cursor.downField(fallback).get[String]
      )
      .toOption

  private def getString(cursor: ExpandedJsonLdCursor, property: Iri) =
    cursor.downField(property).get[String].toOption

  object schema {

    def name(cursor: ExpandedJsonLdCursor): Option[String] =
      getString(cursor, Vocabulary.schema.name, Vocabulary.schema.httpName)

    def description(cursor: ExpandedJsonLdCursor): Option[String] =
      getString(cursor, Vocabulary.schema.description, Vocabulary.schema.httpDescription)
  }

  object rdfs {
    def label(cursor: ExpandedJsonLdCursor): Option[String] = getString(cursor, Vocabulary.rdfs.label)
  }

  object skos {
    def prefLabel(cursor: ExpandedJsonLdCursor): Option[String] = getString(cursor, Vocabulary.skos.prefLabel)
  }

}
