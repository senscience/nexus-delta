package ai.senscience.nexus.delta.plugins.compositeviews.routes

import ai.senscience.nexus.delta.plugins.compositeviews.model.schema
import ai.senscience.nexus.delta.sdk.directives.DeltaSchemeDirectives
import ai.senscience.nexus.delta.sdk.directives.UriDirectives.baseUriPrefix
import ai.senscience.nexus.delta.sdk.model.BaseUri
import org.apache.pekko.http.scaladsl.server.Directives.concat
import org.apache.pekko.http.scaladsl.server.Route

/**
  * Transforms the incoming request to consume the baseUri prefix and rewrite the generic resource endpoint
  */
object CompositeViewsRoutesHandler {

  def apply(schemeDirectives: DeltaSchemeDirectives, routes: Route*)(implicit baseUri: BaseUri): Route =
    (baseUriPrefix(baseUri.prefix) & schemeDirectives.replaceUri("views", schema.iri)) {
      concat(routes*)
    }
}
