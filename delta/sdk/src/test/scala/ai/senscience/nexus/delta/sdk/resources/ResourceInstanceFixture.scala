package ai.senscience.nexus.delta.sdk.resources

import ai.senscience.nexus.delta.rdf.IriOrBNode
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.rdf.graph.Graph
import ai.senscience.nexus.delta.rdf.jsonld.api.{JsonLdApi, TitaniumJsonLdApi}
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue
import ai.senscience.nexus.delta.rdf.jsonld.context.JsonLdContext.keywords
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContext.*
import ai.senscience.nexus.delta.rdf.jsonld.{CompactedJsonLd, ExpandedJsonLd}
import ai.senscience.nexus.delta.sdk.model.jsonld.RemoteContextRef
import ai.senscience.nexus.delta.sdk.resolvers.ResolverContextResolution.ProjectRemoteContext
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sourcing.model.{Label, ProjectRef}
import ai.senscience.nexus.testkit.CirceLiteral
import cats.effect.unsafe.implicits.*
import io.circe.{Json, JsonObject}

trait ResourceInstanceFixture extends CirceLiteral {

  val org: Label                      = Label.unsafe("myorg")
  val proj: Label                     = Label.unsafe("myproj")
  val projectRef: ProjectRef          = ProjectRef(org, proj)
  val myId: IriOrBNode.Iri            = nxv + "myId"
  val staticContext                   = iri"https://bluebrain.github.io/nexus/contexts/metadata.json"
  val nexusContext                    = iri"https://neuroshapes.org"
  val types                           = Set(iri"https://neuroshapes.org/Morphology")
  val source: Json                    =
    json"""
          {
            "@context": [
              "$nexusContext",
              "$staticContext",
              {"@vocab": "https://bluebrain.github.io/nexus/vocabulary/"}
            ],
            "@id": "$myId",
            "@type": "Morphology",
            "name": "Morphology 001"
          }
        """
  private val expandedObj: JsonObject =
    jobj"""
        {
          "@id" : "$myId",
          "@type" : [ "https://neuroshapes.org/Morphology" ],
          "https://bluebrain.github.io/nexus/vocabulary/name" : [ { "@value" : "Morphology 001" } ]
        }"""

  val expanded: ExpandedJsonLd = ExpandedJsonLd.unsafe(myId, expandedObj)
  private val compactedObj     =
    jobj"""
       {
        "@context": [
          "$nexusContext",
          "$staticContext",
          {"@vocab": "https://bluebrain.github.io/nexus/vocabulary/"}
         ],
         "@id": "$myId",
         "@type": "Morphology",
         "name": "Morphology 001"
       }"""

  val compacted: CompactedJsonLd            =
    CompactedJsonLd.unsafe(myId, compactedObj.topContextValueOrEmpty, compactedObj.remove(keywords.context))
  val remoteContexts: Set[RemoteContextRef] = RemoteContextRef(
    Map(
      staticContext -> StaticContext(staticContext, ContextValue.empty),
      nexusContext  -> ProjectRemoteContext(nexusContext, projectRef, 5, ContextValue.empty)
    )
  )

  val graph: Graph = {
    given JsonLdApi = TitaniumJsonLdApi.lenient
    expanded.toGraph.unsafeRunSync()
  }

}
