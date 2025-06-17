package ai.senscience.nexus.delta.rdf.syntax

import ai.senscience.nexus.delta.rdf.graph.{Dot, Graph, NQuads, NTriples}
import ai.senscience.nexus.delta.rdf.jsonld.api.{JsonLdApi, JsonLdOptions}
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.rdf.jsonld.{CompactedJsonLd, ExpandedJsonLd}
import cats.effect.IO

trait JsonLdEncoderSyntax {
  implicit final def jsonLdEncoderSyntax[A: JsonLdEncoder](a: A): JsonLdEncoderOpts[A] = new JsonLdEncoderOpts(a)
}

final class JsonLdEncoderOpts[A](private val value: A)(implicit encoder: JsonLdEncoder[A]) {

  /**
    * Converts a value of type ''A'' to [[CompactedJsonLd]] format using the ''defaultContext'' available on the
    * encoder.
    */
  def toCompactedJsonLd(implicit
      options: JsonLdOptions,
      api: JsonLdApi,
      resolution: RemoteContextResolution
  ): IO[CompactedJsonLd] =
    encoder.compact(value)

  /**
    * Converts a value of type ''A'' to [[ExpandedJsonLd]] format.
    */
  def toExpandedJsonLd(implicit
      options: JsonLdOptions,
      api: JsonLdApi,
      resolution: RemoteContextResolution
  ): IO[ExpandedJsonLd] = encoder.expand(value)

  /**
    * Converts a value of type ''A'' to [[Dot]] format using the ''defaultContext'' available on the encoder.
    */
  def toDot(implicit
      options: JsonLdOptions,
      api: JsonLdApi,
      resolution: RemoteContextResolution
  ): IO[Dot] = encoder.dot(value)

  /**
    * Converts a value of type ''A'' to [[NTriples]] format.
    */
  def toNTriples(implicit
      options: JsonLdOptions,
      api: JsonLdApi,
      resolution: RemoteContextResolution
  ): IO[NTriples] = encoder.ntriples(value)

  /**
    * Converts a value of type ''A'' to [[NQuads]] format.
    */
  def toNQuads(implicit
      options: JsonLdOptions,
      api: JsonLdApi,
      resolution: RemoteContextResolution
  ): IO[NQuads] = encoder.nquads(value)

  /**
    * Converts a value of type ''A'' to [[Graph]] format.
    */
  def toGraph(implicit
      options: JsonLdOptions,
      api: JsonLdApi,
      resolution: RemoteContextResolution
  ): IO[Graph] = encoder.graph(value)
}
