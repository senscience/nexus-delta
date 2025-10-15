package ai.senscience.nexus.delta.plugins.archive

import ai.senscience.nexus.delta.plugins.archive.contexts
import ai.senscience.nexus.delta.plugins.storage.files.contexts as fileContexts
import ai.senscience.nexus.delta.plugins.storage.storages.contexts as storageContexts
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.sdk.RemoteContextResolutionFixtures

trait RemoteContextResolutionFixture extends RemoteContextResolutionFixtures {

  given rcr: RemoteContextResolution =
    loadCoreContexts(
      storageContexts.definition ++ fileContexts.definition ++ contexts.definition
    )
}

object RemoteContextResolutionFixture extends RemoteContextResolutionFixture
