package ai.senscience.nexus.delta.plugins.blazegraph.routes

import ai.senscience.nexus.delta.plugins.blazegraph.model.schema
import ai.senscience.nexus.delta.sdk.directives.DeltaSchemeDirectives
import ai.senscience.nexus.delta.sdk.directives.UriDirectives.baseUriPrefix
import ai.senscience.nexus.delta.sdk.model.BaseUri
import akka.http.scaladsl.server.Directives.concat
import akka.http.scaladsl.server.Route

/**
  * Transforms the incoming request to consume the baseUri prefix and rewrite the generic resource endpoint
  */
object BlazegraphViewsRoutesHandler extends {

  def apply(schemeDirectives: DeltaSchemeDirectives, routes: Route*)(implicit baseUri: BaseUri): Route =
    (baseUriPrefix(baseUri.prefix) & schemeDirectives.replaceUri("views", schema.iri)) {
      concat(routes*)
    }
}
