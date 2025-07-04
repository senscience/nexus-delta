package ai.senscience.nexus.delta.plugins.blazegraph.client

import ai.senscience.nexus.akka.marshalling.RdfMediaTypes.`application/sparql-results+json`
import ai.senscience.nexus.delta.plugins.blazegraph.client.SparqlResults.{Bindings, Head}
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.instances.*
import ai.senscience.nexus.delta.sdk.marshalling.RdfMarshalling
import akka.http.scaladsl.marshalling.ToEntityMarshaller
import io.circe.generic.auto.*
import io.circe.generic.semiauto.*
import io.circe.syntax.*
import io.circe.{Decoder, Encoder}
import org.http4s.Uri

/**
  * Sparql query results representation.
  *
  * @param head
  *   the variables mentioned in the results and may contain a "link" member
  * @param boolean
  *   an optional evaluator for the ASK query
  * @param results
  *   a collection of bindings
  */
final case class SparqlResults(head: Head, results: Bindings, boolean: Option[Boolean] = None) {

  /**
    * Creates a new sparql result which is a merge of the provided results and the current results
    * @param that
    *   the provided head
    */
  def ++(that: SparqlResults): SparqlResults = SparqlResults(head ++ that.head, results ++ that.results)

}

object SparqlResults {

  /**
    * Empty SparqlResults
    */
  val empty: SparqlResults = SparqlResults(Head(List.empty), Bindings(List.empty))

  /**
    * The "head" member gives the variables mentioned in the results and may contain a "link" member.
    *
    * @param vars
    *   an array giving the names of the variables used in the results. These are the projected variables from the
    *   query. A variable is not necessarily given a value in every query solution of the results.
    * @param link
    *   an array of URIs, as strings, to refer for further information. The format and content of these link references
    *   is not defined by this document.
    */
  final case class Head(vars: List[String] = List.empty, link: Option[List[Uri]] = None) {

    /**
      * Creates a new head which is a merge of the provided head and the current head
      * @param that
      *   the provided head
      */
    def ++(that: Head): Head = {
      val newLink = (link ++ that.link).flatten.toList
      Head((vars ++ that.vars).distinct, if (newLink.isEmpty) None else Some(newLink))
    }
  }

  /**
    * The value of the "bindings" member is a map with zero or more elements, one element per query solution. Each query
    * solution is a Binding object. Each key of this object is a variable name from the query solution.
    */
  final case class Bindings(bindings: List[Map[String, Binding]]) {

    /**
      * Creates a new bindings which is a merge of the provided bindings and the current bindings
      * @param that
      *   the provided head
      */
    def ++(that: Bindings): Bindings = Bindings(bindings ++ that.bindings)
  }

  object Bindings {
    def apply(values: Map[String, Binding]*): Bindings = Bindings(values.toList)
  }

  /**
    * Encodes an RDF term
    *
    * @param `type`
    *   the type of the term
    * @param value
    *   the value of the term
    * @param `xml:lang`
    *   the language tag (when the term is a literal)
    * @param datatype
    *   the data type information of the term
    */
  final case class Binding(
      `type`: String,
      value: String,
      `xml:lang`: Option[String] = None,
      datatype: Option[String] = None
  )

  implicit final val sparqlResultsEncoder: Encoder[SparqlResults] = deriveEncoder[SparqlResults]

  private val askResultDecoder: Decoder[SparqlResults] =
    Decoder.instance(_.get[Boolean]("boolean").map { boolean => SparqlResults(Head(), Bindings(), Some(boolean)) })

  implicit final val sparqlResultsDecoder: Decoder[SparqlResults] = {
    val default = deriveDecoder[SparqlResults]
    Decoder.instance(hc => default(hc) orElse askResultDecoder(hc))
  }

  implicit def sparqlResultsMarshaller(implicit ordering: JsonKeyOrdering): ToEntityMarshaller[SparqlResults] =
    RdfMarshalling.customContentTypeJsonMarshaller(`application/sparql-results+json`).compose(_.asJson)

}
