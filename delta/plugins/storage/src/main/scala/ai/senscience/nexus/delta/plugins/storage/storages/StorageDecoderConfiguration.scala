package ai.senscience.nexus.delta.plugins.storage.storages

import ai.senscience.nexus.delta.plugins.storage.storages.model.StorageType
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, JsonLdContext, RemoteContextResolution}
import ai.senscience.nexus.delta.rdf.jsonld.decoder.Configuration
import cats.effect.IO

private[storages] object StorageDecoderConfiguration {

  def apply(implicit rcr: RemoteContextResolution): IO[Configuration] =
    for {
      contextValue  <- IO.delay { ContextValue(contexts.storages) }
      jsonLdContext <- JsonLdContext(contextValue)
    } yield {
      val ctx = jsonLdContext
        .addAlias("DiskStorageFields", StorageType.DiskStorage.iri)
        .addAlias("S3StorageFields", StorageType.S3Storage.iri)
      Configuration(ctx, "id")
    }
}
