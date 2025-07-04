package ai.senscience.nexus.delta.rdf

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.graph.{NQuads, NTriples}
import ai.senscience.nexus.delta.rdf.jsonld.context.JsonLdContext.keywords
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolutionError
import ai.senscience.nexus.delta.rdf.query.SparqlQuery.SparqlConstructQuery
import io.circe.Encoder
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveConfiguredEncoder
import org.apache.jena.riot.Lang

sealed abstract class RdfError(val reason: String, details: Option[String] = None) extends Exception {
  override def fillInStackTrace(): RdfError = this
  override def getMessage: String           = details.fold(reason)(d => s"$reason\nDetails: $d")
}

object RdfError {

  /**
    * An unexpected JSON-LD document
    */
  final case class UnexpectedJsonLd(details: String) extends RdfError("Unexpected JSON-LD document.", Some(details))

  /**
    * An unexpected JSON-LD @context document
    */
  final case class UnexpectedJsonLdContext(details: String)
      extends RdfError("Unexpected JSON-LD @context document.", Some(details))

  /**
    * An error while resolving remote @context
    */
  final case class RemoteContextError(error: RemoteContextResolutionError)
      extends RdfError(error.getMessage, error.getDetails)

  /**
    * An unexpected conversion error
    */
  final case class ConversionError(details: String, stage: String)
      extends RdfError(s"Error on the conversion stage '$stage'", Some(details))

  final case class ParsingError(lang: String, message: String, rootNode: IriOrBNode, headValue: String)
      extends RdfError(
        s"Error while parsing $lang for id $rootNode: '$message'",
        Some(s"Value:\n$headValue...")
      )

  object ParsingError {

    private val limit                                            = 500
    def apply(message: String, nTriples: NTriples): ParsingError =
      ParsingError(
        Lang.NTRIPLES.getName,
        message,
        nTriples.rootNode,
        nTriples.value.substring(0, limit)
      )

    def apply(message: String, nQuads: NQuads): ParsingError =
      ParsingError(
        Lang.NQUADS.getName,
        message,
        nQuads.rootNode,
        nQuads.value.substring(0, limit)
      )
  }

  /**
    * Missing required predicate.
    */
  final case class MissingPredicate(predicate: Iri)
      extends RdfError(s"Required predicate $predicate is missing from graph.", None)

  /**
    * Invalid Iri
    */
  final case object InvalidIri extends RdfError(s"keyword '${keywords.id}' could not be converted to an Iri")
  type InvalidIri = InvalidIri.type

  /**
    * Unexpected Iri value
    */
  final case class UnexpectedIriOrBNode(expected: IriOrBNode, found: IriOrBNode)
      extends RdfError(s"Unexpected Iri or blank node value. Expected '$expected', found '$found'")

  final case class SparqlConstructQueryError(query: SparqlConstructQuery, rootNode: IriOrBNode, message: String)
      extends RdfError(
        s"The query '${query.value}' on graph with root node '$rootNode' resulted in a error: '$message'"
      )

  implicit private val config: Configuration = Configuration.default.withDiscriminator(keywords.tpe)

  implicit val rdfErrorEncoder: Encoder.AsObject[RdfError] = deriveConfiguredEncoder[RdfError]
}
