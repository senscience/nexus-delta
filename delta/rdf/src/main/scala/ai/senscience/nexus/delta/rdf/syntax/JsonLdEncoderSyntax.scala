package ai.senscience.nexus.delta.rdf.syntax

import ai.senscience.nexus.delta.rdf.graph.{Dot, Graph, NQuads, NTriples}
import ai.senscience.nexus.delta.rdf.jsonld.api.JsonLdApi
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.rdf.jsonld.{CompactedJsonLd, ExpandedJsonLd}
import cats.effect.IO

trait JsonLdEncoderSyntax {

  extension [A](value: A)(using encoder: JsonLdEncoder[A]) {

    /**
      * Converts a value of type ''A'' to [[CompactedJsonLd]] format using the ''defaultContext'' available on the
      * encoder.
      */
    def toCompactedJsonLd(using JsonLdApi, RemoteContextResolution): IO[CompactedJsonLd] =
      encoder.compact(value)

    /**
      * Converts a value of type ''A'' to [[ExpandedJsonLd]] format.
      */
    def toExpandedJsonLd(using JsonLdApi, RemoteContextResolution): IO[ExpandedJsonLd] = encoder.expand(value)

    /**
      * Converts a value of type ''A'' to [[Dot]] format using the ''defaultContext'' available on the encoder.
      */
    def toDot(using JsonLdApi, RemoteContextResolution): IO[Dot] = encoder.dot(value)

    /**
      * Converts a value of type ''A'' to [[NTriples]] format.
      */
    def toNTriples(using JsonLdApi, RemoteContextResolution): IO[NTriples] = encoder.ntriples(value)

    /**
      * Converts a value of type ''A'' to [[NQuads]] format.
      */
    def toNQuads(using JsonLdApi, RemoteContextResolution): IO[NQuads] = encoder.nquads(value)

    /**
      * Converts a value of type ''A'' to [[Graph]] format.
      */
    def toGraph(using JsonLdApi, RemoteContextResolution): IO[Graph] = encoder.graph(value)
  }

}
