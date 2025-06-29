package ai.senscience.nexus.delta.plugins.graph.analytics

import ai.senscience.nexus.delta.rdf.Vocabulary
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.testkit.scalatest.ClasspathResources

trait ContextFixtures extends ClasspathResources {
  implicit val rcr: RemoteContextResolution =
    RemoteContextResolution.fixed(
      contexts.relationships         -> jsonContentOf("contexts/relationships.json").topContextValueOrEmpty,
      contexts.properties            -> jsonContentOf("contexts/properties.json").topContextValueOrEmpty,
      Vocabulary.contexts.statistics -> jsonContentOf("contexts/statistics.json").topContextValueOrEmpty,
      Vocabulary.contexts.error      -> jsonContentOf("contexts/error.json").topContextValueOrEmpty
    )
}
