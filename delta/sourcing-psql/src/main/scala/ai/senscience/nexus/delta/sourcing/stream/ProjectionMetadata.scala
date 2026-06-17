package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec

/**
  * Defines metadata for a projection
  * @param module
  *   the module the projection lives in
  * @param name
  *   the name of the projection
  * @param project
  *   the optional project linked to the projection (ex: the project of a view)
  * @param resourceId
  *   the optional resource identifier linked to the projection (ex: a view)
  */
final case class ProjectionMetadata(
    module: String,
    name: String,
    project: Option[ProjectRef],
    resourceId: Option[Iri]
) {
  def fullName: String = s"$module/$name"
}

object ProjectionMetadata {

  /**
    * Metadata for a projection that is not related to a given project or resource
    * @param module
    *   the module the projection lives in
    * @param name
    *   the name of the projection
    */
  def apply(module: String, name: String): ProjectionMetadata = ProjectionMetadata(module, name, None, None)

  def apply(module: String, name: String, project: ProjectRef, resourceId: Iri): ProjectionMetadata =
    ProjectionMetadata(module, name, Some(project), Some(resourceId))

  given Codec[ProjectionMetadata] = deriveCodec
}
