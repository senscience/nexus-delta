package ai.senscience.nexus.delta.sdk.instances

import ai.senscience.nexus.delta.sdk.jsonld.IriEncoder
import ai.senscience.nexus.delta.sdk.model.{BaseUri, ResourceAccess}
import ai.senscience.nexus.delta.sdk.syntax.*
import ch.epfl.bluebrain.nexus.delta.rdf.IriOrBNode.Iri
import ch.epfl.bluebrain.nexus.delta.sourcing.model.ProjectRef

trait ProjectRefInstances {

  implicit final val projectRefIriEncoder: IriEncoder[ProjectRef] = new IriEncoder[ProjectRef] {
    override def apply(value: ProjectRef)(implicit base: BaseUri): Iri =
      ResourceAccess.project(value).uri.toIri
  }
}
