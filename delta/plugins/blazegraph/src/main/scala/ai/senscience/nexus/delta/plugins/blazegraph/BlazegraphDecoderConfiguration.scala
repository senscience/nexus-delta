package ai.senscience.nexus.delta.plugins.blazegraph

import ai.senscience.nexus.delta.plugins.blazegraph.model.{contexts, BlazegraphViewType}
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, JsonLdContext, RemoteContextResolution}
import ai.senscience.nexus.delta.rdf.jsonld.decoder.Configuration
import cats.effect.IO

object BlazegraphDecoderConfiguration {

  def apply(implicit rcr: RemoteContextResolution): IO[Configuration] = for {
    contextValue  <- IO.delay { ContextValue(contexts.blazegraph) }
    jsonLdContext <- JsonLdContext(contextValue)
  } yield {
    val enhancedJsonLdContext = jsonLdContext
      .addAliasIdType("IndexingBlazegraphViewValue", BlazegraphViewType.IndexingBlazegraphView.tpe)
      .addAliasIdType("AggregateBlazegraphViewValue", BlazegraphViewType.AggregateBlazegraphView.tpe)
    Configuration(enhancedJsonLdContext, "id")
  }

}
