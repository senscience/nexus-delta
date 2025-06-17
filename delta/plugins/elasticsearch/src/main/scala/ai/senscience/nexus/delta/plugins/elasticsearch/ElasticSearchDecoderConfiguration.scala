package ai.senscience.nexus.delta.plugins.elasticsearch

import ai.senscience.nexus.delta.plugins.elasticsearch.model.contexts
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, JsonLdContext, RemoteContextResolution}
import ai.senscience.nexus.delta.rdf.jsonld.decoder.Configuration
import cats.effect.IO

private[elasticsearch] object ElasticSearchDecoderConfiguration {

  /**
    * @return
    *   a decoder configuration that uses the elasticsearch context
    */
  def apply(implicit rcr: RemoteContextResolution): IO[Configuration] =
    for {
      contextValue  <- IO { ContextValue(contexts.elasticsearch) }
      jsonLdContext <- JsonLdContext(contextValue)
    } yield Configuration(jsonLdContext, "id")

}
