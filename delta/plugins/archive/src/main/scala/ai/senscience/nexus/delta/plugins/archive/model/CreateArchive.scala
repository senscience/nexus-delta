package ai.senscience.nexus.delta.plugins.archive.model

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.ProjectRef

/**
  * Command for the creation of an archive.
  *
  * @param id
  *   the identifier of the archive
  * @param project
  *   the parent project
  * @param value
  *   the archive value
  * @param subject
  *   the identity associated with this command
  */
final case class CreateArchive(
    id: Iri,
    project: ProjectRef,
    value: ArchiveValue,
    subject: Subject
)
