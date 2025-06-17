package ai.senscience.nexus.delta.plugins.storage

import ai.senscience.nexus.delta.plugins.storage.files.contexts as fileContexts
import ai.senscience.nexus.delta.plugins.storage.storages.contexts as storageContexts
import ai.senscience.nexus.delta.rdf.Vocabulary
import ai.senscience.nexus.delta.rdf.jsonld.api.{JsonLdApi, TitaniumJsonLdApi}
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import ch.epfl.bluebrain.nexus.delta.kernel.utils.ClasspathResourceLoader

trait RemoteContextResolutionFixture {

  import RemoteContextResolutionFixture.*
  implicit val api: JsonLdApi = TitaniumJsonLdApi.strict

  implicit val rcr: RemoteContextResolution = RemoteContextResolution.fixedIO(
    storageContexts.storages         -> ContextValue.fromFile("contexts/storages.json"),
    storageContexts.storagesMetadata -> ContextValue.fromFile("contexts/storages-metadata.json"),
    fileContexts.files               -> ContextValue.fromFile("contexts/files.json"),
    Vocabulary.contexts.metadata     -> ContextValue.fromFile("contexts/metadata.json"),
    Vocabulary.contexts.error        -> ContextValue.fromFile("contexts/error.json"),
    Vocabulary.contexts.tags         -> ContextValue.fromFile("contexts/tags.json"),
    Vocabulary.contexts.search       -> ContextValue.fromFile("contexts/search.json")
  )
}

object RemoteContextResolutionFixture {
  implicit private val loader: ClasspathResourceLoader =
    ClasspathResourceLoader.withContext(getClass)
}
