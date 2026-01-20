package ai.senscience.nexus.delta.rdf.jsonld.context

import ai.senscience.nexus.delta.rdf.Fixtures
import ai.senscience.nexus.delta.rdf.implicits.*
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContext.StaticContext
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolutionError.RemoteContextNotFound
import ai.senscience.nexus.testkit.mu.NexusSuite

class RemoteContextResolutionSuite extends NexusSuite with Fixtures {

  private val input = jsonContentOf("jsonld/context/input-with-remote-context.json")

  test("A remote context resolution succeeds") {
    val expected = remoteContexts.map { case (iri, context) => iri -> StaticContext(iri, context) }
    remoteResolution(input).assertEquals(expected)
  }

  test("A remote context resolution fails to resolve when some context does not exist") {
    val excluded         = iri"http://senscience.ai/cöntéxt/3"
    val ctxValuesMap     = remoteContexts - excluded
    val remoteResolution = RemoteContextResolution.fixed(ctxValuesMap.toSeq*)
    remoteResolution(input).interceptEquals(RemoteContextNotFound(excluded))
  }

  test("A remote context resolution merges and resolves") {
    val excluded           = iri"http://senscience.ai/cöntéxt/3"
    val ctxValue           = remoteContexts(excluded)
    val ctxValuesMap       = remoteContexts - excluded
    val excludedResolution = RemoteContextResolution.fixed(excluded -> ctxValue)
    val restResolution     = RemoteContextResolution.fixed(ctxValuesMap.toSeq*)
    restResolution.merge(excludedResolution).resolve(excluded).assertEquals(StaticContext(excluded, ctxValue))
  }
}
