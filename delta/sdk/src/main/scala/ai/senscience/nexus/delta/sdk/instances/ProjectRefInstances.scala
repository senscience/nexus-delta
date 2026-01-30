package ai.senscience.nexus.delta.sdk.instances

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sdk.jsonld.IriEncoder
import ai.senscience.nexus.delta.sdk.model.{BaseUri, ResourceAccess}
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sourcing.model.ProjectRef

trait ProjectRefInstances {

  given IriEncoder[ProjectRef] = new IriEncoder[ProjectRef] {
    override def apply(value: ProjectRef)(using base: BaseUri): Iri =
      ResourceAccess.project(value).uri.toIri
  }
}
