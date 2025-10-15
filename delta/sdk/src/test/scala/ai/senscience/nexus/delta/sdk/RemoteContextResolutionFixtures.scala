package ai.senscience.nexus.delta.sdk

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.sdk.schemas.contexts
import ai.senscience.nexus.testkit.scalatest.ClasspathLoader

trait RemoteContextResolutionFixtures extends ClasspathLoader {

  def loadCoreContexts(extraContexts: Set[(Iri, String)]): RemoteContextResolution =
    RemoteContextResolution.loadResourcesUnsafe(coreContexts ++ extraContexts)

  def loadCoreContextsAndSchemas: RemoteContextResolution = loadCoreContexts(contexts.definition)

}
