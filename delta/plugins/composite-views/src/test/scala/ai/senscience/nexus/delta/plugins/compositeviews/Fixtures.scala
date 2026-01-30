package ai.senscience.nexus.delta.plugins.compositeviews

import ai.senscience.nexus.delta.plugins.compositeviews.model.contexts
import ai.senscience.nexus.delta.rdf.jsonld.api.{JsonLdApi, TitaniumJsonLdApi}
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.sdk.RemoteContextResolutionFixtures
import ai.senscience.nexus.delta.sdk.syntax.*

trait Fixtures extends RemoteContextResolutionFixtures {

  given api: JsonLdApi = TitaniumJsonLdApi.strict

  given rcr: RemoteContextResolution = loadCoreContexts(
    contexts.definition ++ Set(iri"http://music.com/context" -> "indexing/music-context.json")
  )
}
