package ai.senscience.nexus.delta.sdk.jsonld

import ai.senscience.nexus.delta.kernel.error.Rejection
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sdk.model.{IdSegment, IdSegmentRef}
import ai.senscience.nexus.delta.sdk.projects.model.ProjectContext
import ai.senscience.nexus.delta.sourcing.model.ResourceRef
import cats.effect.IO

final class ExpandIri[R <: Rejection](val onError: String => R) extends AnyVal {

  /**
    * Expand the given segment to an Iri using the provided project if necessary
    * @param segment
    *   the to translate to an Iri
    * @param projectContext
    *   the project context
    */
  def apply(segment: IdSegment, projectContext: ProjectContext): IO[Iri] =
    apply(IdSegmentRef(segment), projectContext).map(_.iri)

  /**
    * Expand the given segment to a ResourceRef using the provided project if necessary and applying the revision to get
    * a resource reference
    *
    * @param segment
    *   the segment to translate to an Iri with its optional rev/tag
    * @param projectContext
    *   the project context
    */
  def apply(segment: IdSegmentRef, projectContext: ProjectContext): IO[ResourceRef] =
    IO.fromOption(
      segment.value.toIri(projectContext.apiMappings, projectContext.base).map { iri =>
        segment match {
          case IdSegmentRef.Latest(_)        => ResourceRef.Latest(iri)
          case IdSegmentRef.Revision(_, rev) => ResourceRef.Revision(iri, rev)
          case IdSegmentRef.Tag(_, tag)      => ResourceRef.Tag(iri, tag)
        }
      }
    )(onError(segment.value.asString))
}
