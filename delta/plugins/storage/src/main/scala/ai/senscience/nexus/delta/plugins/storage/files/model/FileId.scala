package ai.senscience.nexus.delta.plugins.storage.files.model

import ai.senscience.nexus.delta.plugins.storage.files.model.FileId.iriExpander
import ai.senscience.nexus.delta.plugins.storage.files.model.FileRejection.InvalidFileId
import cats.effect.IO
import ch.epfl.bluebrain.nexus.delta.rdf.IriOrBNode.Iri
import ch.epfl.bluebrain.nexus.delta.sdk.jsonld.ExpandIri
import ch.epfl.bluebrain.nexus.delta.sdk.model.{IdSegment, IdSegmentRef}
import ch.epfl.bluebrain.nexus.delta.sdk.projects.model.ProjectContext
import ch.epfl.bluebrain.nexus.delta.sourcing.model.Tag.UserTag
import ch.epfl.bluebrain.nexus.delta.sourcing.model.{ProjectRef, ResourceRef}

final case class FileId(id: IdSegmentRef, project: ProjectRef) {
  def expandIri(fetchContext: ProjectRef => IO[ProjectContext]): IO[(Iri, ProjectContext)] =
    fetchContext(project).flatMap(pc => iriExpander(id.value, pc).map(iri => (iri, pc)))

  def expandRef(fetchContext: ProjectRef => IO[ProjectContext]): IO[ResourceRef] =
    fetchContext(project).flatMap { pc =>
      iriExpander(id.value, pc).map { iri =>
        id match {
          case IdSegmentRef.Latest(_)        => ResourceRef.Latest(iri)
          case IdSegmentRef.Revision(_, rev) => ResourceRef.Revision(iri, rev)
          case IdSegmentRef.Tag(_, tag)      => ResourceRef.Tag(iri, tag)
        }
      }
    }
}

object FileId {
  def apply(ref: ResourceRef, project: ProjectRef): FileId            = FileId(IdSegmentRef(ref), project)
  def apply(id: IdSegment, tag: UserTag, project: ProjectRef): FileId = FileId(IdSegmentRef(id, tag), project)
  def apply(id: IdSegment, rev: Int, project: ProjectRef): FileId     = FileId(IdSegmentRef(id, rev), project)
  def apply(id: IdSegment, project: ProjectRef): FileId               = FileId(IdSegmentRef(id), project)

  val iriExpander: ExpandIri[InvalidFileId] = new ExpandIri(InvalidFileId.apply)
}
