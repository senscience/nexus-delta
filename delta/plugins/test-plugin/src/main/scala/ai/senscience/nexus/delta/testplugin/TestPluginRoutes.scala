package ai.senscience.nexus.delta.testplugin

import ai.senscience.nexus.delta.sdk.model.BaseUri
import akka.http.scaladsl.server.Directives.*
import akka.http.scaladsl.server.Route

class TestPluginRoutes(baseUri: BaseUri) {
  def routes: Route =
    pathPrefix("test-plugin") {
      concat(
        get {
          complete(baseUri.toString)
        }
      )
    }
}
