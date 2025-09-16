package ai.senscience.nexus.delta.plugins.compositeviews.model

import ai.senscience.nexus.delta.plugins.compositeviews.model.CompositeViewProjection.idTemplating
import ai.senscience.nexus.delta.rdf.jsonld.decoder.JsonLdDecoder
import ai.senscience.nexus.delta.rdf.jsonld.decoder.JsonLdDecoderError.ParsingFailure
import ai.senscience.nexus.delta.rdf.query.SparqlQuery.SparqlConstructQuery
import ai.senscience.nexus.delta.sdk.syntax.*
import cats.syntax.all.*

import java.util.regex.Pattern.quote

object TemplateSparqlConstructQuery {
  private val fakeIri = iri"http://localhost/id"

  /**
    * Constructs a [[SparqlConstructQuery]] verifying that the passed ''value'' contains the id templating and that it
    * is a valid CONSTRUCT query
    */
  final def apply(value: String): Either[String, SparqlConstructQuery] =
    if !value.contains(idTemplating) then
      Left(s"Required templating '$idTemplating' in the provided SPARQL query is not found")
    else
      SparqlConstructQuery(value.replaceAll(quote(idTemplating), fakeIri.rdfFormat))
        .as(SparqlConstructQuery.unsafe(value))

  implicit val sparqlConstructQueryJsonLdDecoder: JsonLdDecoder[SparqlConstructQuery] =
    JsonLdDecoder.stringJsonLdDecoder.andThen { case (cursor, str) =>
      TemplateSparqlConstructQuery(str).leftMap(reason =>
        ParsingFailure("SparqlConstructQuery", str, cursor.history, reason)
      )
    }
}
