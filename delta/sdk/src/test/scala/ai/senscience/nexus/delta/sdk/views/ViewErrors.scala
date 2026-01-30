package ai.senscience.nexus.delta.sdk.views

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.testkit.CirceLiteral.*
import io.circe.Json

trait ViewErrors {

  def viewType: String

  def viewIsNotDeprecatedError(id: Iri): Json =
    json"""
      {
        "@context": "https://bluebrain.github.io/nexus/contexts/error.json",
        "@type": "ViewIsNotDeprecated",
        "reason": "$viewType view '$id' is not deprecated."
      }
    """

  def viewIsDeprecatedError(id: Iri): Json =
    json"""
      {
        "@context": "https://bluebrain.github.io/nexus/contexts/error.json",
        "@type": "ViewIsDeprecated",
        "reason": "$viewType view '$id' is deprecated."
      }
    """

  def viewNotFoundError(id: Iri, project: ProjectRef): Json =
    json"""
      {
        "@context": "https://bluebrain.github.io/nexus/contexts/error.json",
        "@type": "ResourceNotFound",
        "reason": "$viewType view '$id' not found in project '$project'."
      }
    """

}

object ElasticSearchViewErrors extends ViewErrors {
  override def viewType: String = "ElasticSearch"
}

object BlazegraphViewErrors extends ViewErrors {
  override def viewType: String = "Blazegraph"
}

object CompositeViewErrors extends ViewErrors {
  override def viewType: String = "Composite"
}
