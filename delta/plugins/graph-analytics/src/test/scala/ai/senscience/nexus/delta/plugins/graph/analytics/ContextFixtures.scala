package ai.senscience.nexus.delta.plugins.graph.analytics

import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.sdk.RemoteContextResolutionFixtures

trait ContextFixtures extends RemoteContextResolutionFixtures {

  given rcr: RemoteContextResolution = loadCoreContexts(contexts.definition)
}
