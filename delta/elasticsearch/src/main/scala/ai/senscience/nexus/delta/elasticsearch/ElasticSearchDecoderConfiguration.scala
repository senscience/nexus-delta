package ai.senscience.nexus.delta.elasticsearch

import ai.senscience.nexus.delta.elasticsearch.model.contexts
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, JsonLdContext, RemoteContextResolution}
import ai.senscience.nexus.delta.rdf.jsonld.decoder.Configuration
import cats.effect.IO

private[elasticsearch] object ElasticSearchDecoderConfiguration {

  /**
    * @return
    *   a decoder configuration that uses the elasticsearch context
    */
  def apply(using RemoteContextResolution): IO[Configuration] =
    JsonLdContext(ContextValue(contexts.elasticsearch)).map { jsonLdContext =>
      Configuration(jsonLdContext, "id")
    }

}
