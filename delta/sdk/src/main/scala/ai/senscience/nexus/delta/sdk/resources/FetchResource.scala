package ai.senscience.nexus.delta.sdk.resources

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sdk.model.Fetch.FetchF
import ai.senscience.nexus.delta.sdk.model.IdSegmentRef
import ai.senscience.nexus.delta.sdk.model.IdSegmentRef.{Latest, Revision, Tag}
import ai.senscience.nexus.delta.sdk.resources.model.*
import ai.senscience.nexus.delta.sdk.resources.model.ResourceRejection.{ResourceNotFound, RevisionNotFound, TagNotFound}
import ai.senscience.nexus.delta.sourcing.ScopedEventLogReadOnly
import ai.senscience.nexus.delta.sourcing.model.{ProjectRef, ResourceRef}
import cats.effect.IO

trait FetchResource {

  /** Fetch the referenced resource in the given project */
  def fetch(ref: ResourceRef, project: ProjectRef): FetchF[Resource]

  def stateOrNotFound(id: IdSegmentRef, iri: Iri, ref: ProjectRef): IO[ResourceState]

}

object FetchResource {

  def apply(
      log: ScopedEventLogReadOnly[Iri, ResourceState, ResourceRejection]
  ): FetchResource = {

    def notFound(iri: Iri, ref: ProjectRef) = ResourceNotFound(iri, ref)

    new FetchResource {
      override def fetch(ref: ResourceRef, project: ProjectRef): FetchF[Resource] = {
        stateOrNotFound(IdSegmentRef(ref), ref.iri, project).attempt
          .map(_.toOption)
          .map(_.map(_.toResource))
      }

      override def stateOrNotFound(id: IdSegmentRef, iri: Iri, ref: ProjectRef): IO[ResourceState] =
        id match {
          case Latest(_)        => log.stateOr(ref, iri, notFound(iri, ref))
          case Revision(_, rev) => log.stateOr(ref, iri, rev, notFound(iri, ref), RevisionNotFound)
          case Tag(_, tag)      => log.stateOr(ref, iri, tag, notFound(iri, ref), TagNotFound(tag))
        }
    }

  }

}
