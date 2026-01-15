package ai.senscience.nexus.delta.rdf.jsonld.api

import ai.senscience.nexus.delta.rdf.ExplainResult
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, JsonLdContext, RemoteContextResolution}
import cats.effect.IO
import io.circe.{Json, JsonObject}
import org.apache.jena.sparql.core.DatasetGraph

/**
  * Json-LD high level API as defined in the ''JsonLdProcessor'' interface of the Json-LD spec.
  *
  * Interface definition for compact, expand, frame, toRdf, fromRdf:
  * https://www.w3.org/TR/json-ld11-api/#the-jsonldprocessor-interface Interface definition for frame:
  * https://www.w3.org/TR/json-ld11-framing/#jsonldprocessor
  */
trait JsonLdApi {
  private[rdf] def compact(
      input: Json,
      ctx: ContextValue
  )(using RemoteContextResolution): IO[JsonObject]

  private[rdf] def expand(
      input: Json
  )(using RemoteContextResolution): IO[Seq[JsonObject]]

  /**
    * Performs the expand operation and provides details on its execution
    */
  private[rdf] def explainExpand(
      input: Json
  )(using rcr: RemoteContextResolution): IO[ExplainResult[Seq[JsonObject]]]

  private[rdf] def frame(
      input: Json,
      frame: Json
  )(using RemoteContextResolution): IO[JsonObject]

  private[rdf] def toRdf(input: Json): IO[DatasetGraph]

  private[rdf] def fromRdf(input: DatasetGraph): IO[Seq[JsonObject]]

  private[rdf] def context(
      value: ContextValue
  )(using RemoteContextResolution): IO[JsonLdContext]

}
