package ai.senscience.nexus.akka.marshalling

import akka.http.scaladsl.model.HttpCharsets.*
import akka.http.scaladsl.model.MediaType

/**
  * Collection of media types specific to RDF.
  */
// $COVERAGE-OFF$
object RdfMediaTypes {
  final val `text/turtle`: MediaType.WithFixedCharset =
    MediaType.textWithFixedCharset("turtle", `UTF-8`, "ttl")

  final val `application/rdf+xml`: MediaType.WithFixedCharset =
    MediaType.applicationWithFixedCharset("rdf+xml", `UTF-8`, "xml")

  final val `application/n-triples`: MediaType.WithFixedCharset =
    MediaType.applicationWithFixedCharset("n-triples", `UTF-8`, "nt")

  final val `application/n-quads`: MediaType.WithFixedCharset =
    MediaType.applicationWithFixedCharset("n-quads", `UTF-8`, "nq")

  final val `application/ld+json`: MediaType.WithFixedCharset =
    MediaType.applicationWithFixedCharset("ld+json", `UTF-8`, "jsonld")

  final val `application/sparql-results+json`: MediaType.WithFixedCharset =
    MediaType.applicationWithFixedCharset("sparql-results+json", `UTF-8`, "json")

  final val `application/sparql-results+xml`: MediaType.WithFixedCharset =
    MediaType.applicationWithFixedCharset("sparql-results+xml", `UTF-8`, "xml")

  final val `application/sparql-query`: MediaType.WithFixedCharset =
    MediaType.applicationWithFixedCharset("sparql-query", `UTF-8`)

  final val `application/sparql-update`: MediaType.WithFixedCharset =
    MediaType.applicationWithFixedCharset("sparql-update", `UTF-8`)

  final val `text/vnd.graphviz`: MediaType.WithFixedCharset =
    MediaType.textWithFixedCharset("vnd.graphviz", `UTF-8`, "dot")
}
// $COVERAGE-ON$
