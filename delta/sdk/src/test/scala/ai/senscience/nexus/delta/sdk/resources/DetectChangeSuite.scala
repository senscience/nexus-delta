package ai.senscience.nexus.delta.sdk.resources

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.rdf.graph.Graph
import ai.senscience.nexus.delta.rdf.jsonld.api.{JsonLdApi, TitaniumJsonLdApi}
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue
import ai.senscience.nexus.delta.rdf.jsonld.{CompactedJsonLd, ExpandedJsonLd}
import ai.senscience.nexus.delta.sdk.jsonld.JsonLdAssembly
import ai.senscience.nexus.delta.sdk.model.jsonld.RemoteContextRef
import ai.senscience.nexus.delta.sdk.model.jsonld.RemoteContextRef.StaticContextRef
import ai.senscience.nexus.testkit.mu.NexusSuite
import io.circe.syntax.KeyOps
import io.circe.{Json, JsonObject}

class DetectChangeSuite extends NexusSuite {

  implicit val jsonLdApi: JsonLdApi = TitaniumJsonLdApi.lenient

  private val id = nxv + "id"

  private val source                                = Json.obj("source" := "value")
  private val compactedValue                        = JsonObject("field" := "value")
  private val compacted                             = CompactedJsonLd.unsafe(id, ContextValue(nxv + "context"), compactedValue)
  private val remoteContexts: Set[RemoteContextRef] = Set(StaticContextRef(nxv + "static"))

  private val jsonld = JsonLdAssembly(
    nxv + "id",
    source,
    compacted,
    ExpandedJsonLd.empty,
    Graph.empty(id),
    remoteContexts
  )

  private val current = DetectChange.Current(Set.empty, source, compacted, remoteContexts)

  private val detectChange = DetectChange(enabled = true)

  test("No change is detected") {
    detectChange(jsonld, current).assertEquals(false)
  }

  test("A change is detected if resource types are different") {
    val otherTypes: Set[Iri] = Set(nxv + "another-type")
    detectChange(jsonld, current.copy(types = otherTypes)).assertEquals(true)
  }

  test("A change is detected if the remote contexts are different") {
    val otherRemoteContexts: Set[RemoteContextRef] = Set(StaticContextRef(nxv + "another-static"))
    detectChange(jsonld, current.copy(remoteContexts = otherRemoteContexts)).assertEquals(true)
  }

  test("A change is detected if the local contexts are different") {
    val otherLocalContext = CompactedJsonLd.unsafe(id, ContextValue(nxv + "another-context"), compactedValue)
    detectChange(jsonld, current.copy(compacted = otherLocalContext)).assertEquals(true)
  }

  test("A change is detected if the source differs") {
    val otherSource = Json.obj("source" := "another-value")
    detectChange(jsonld, current.copy(source = otherSource)).assertEquals(true)
  }

}
