package ai.senscience.nexus.delta.rdf.syntax

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import org.http4s.Uri

trait UriSyntax {

  extension (uri: Uri) {

    /**
      * Constructs an [[Iri]] from a [[Uri]]
      */
    def toIri: Iri = Iri.unsafe(uri.toString)

    /**
      * Add a final slash to the uri
      */
    def finalSlash: Uri = uri.withPath(uri.path.addEndsWithSlash)
  }

  extension (path: Uri.Path) {

    /**
      * @return
      *   a path last segment
      */
    def lastSegment: Option[String] = path.segments.lastOption.map(_.toString)
  }
}
