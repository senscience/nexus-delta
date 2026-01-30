package ai.senscience.nexus.delta.plugins.blazegraph

import ai.senscience.nexus.delta.plugins.blazegraph.model.BlazegraphViewValue
import ai.senscience.nexus.delta.plugins.blazegraph.model.contexts
import ai.senscience.nexus.delta.rdf.jsonld.api.{JsonLdApi, TitaniumJsonLdApi}
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.sdk.RemoteContextResolutionFixtures
import cats.effect.IO

trait Fixtures extends RemoteContextResolutionFixtures {

  given api: JsonLdApi = TitaniumJsonLdApi.strict

  given rcr: RemoteContextResolution = loadCoreContexts(contexts.definition)

  def alwaysValidate: ValidateBlazegraphView = (_: BlazegraphViewValue) => IO.unit
}
