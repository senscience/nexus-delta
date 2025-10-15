package ai.senscience.nexus.delta.sdk.utils

import ai.senscience.nexus.delta.rdf.jsonld.api.{JsonLdApi, TitaniumJsonLdApi}
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.sdk.{organizations, permissions, schemas, RemoteContextResolutionFixtures}

trait Fixtures extends RemoteContextResolutionFixtures {
  given api: JsonLdApi = TitaniumJsonLdApi.strict

  given rcr: RemoteContextResolution =
    loadCoreContexts(
      schemas.contexts.definition ++
        organizations.contexts.definition ++
        permissions.contexts.definition
    )

}
