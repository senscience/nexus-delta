package ai.senscience.nexus.delta.testplugin

import ai.senscience.nexus.delta.sdk.model.BaseUri
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route

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
