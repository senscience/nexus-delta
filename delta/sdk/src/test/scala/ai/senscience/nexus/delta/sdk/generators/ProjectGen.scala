package ai.senscience.nexus.delta.sdk.generators

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.rdf.syntax.iriStringContextSyntax
import ai.senscience.nexus.delta.sdk.ProjectResource
import ai.senscience.nexus.delta.sdk.projects.model.*
import ai.senscience.nexus.delta.sourcing.model.Identity.{Anonymous, Subject}
import ai.senscience.nexus.delta.sourcing.model.Label

import java.time.Instant
import java.util.UUID

object ProjectGen {

  val defaultApiMappings: ApiMappings = ApiMappings(
    "nxv" -> iri"https://bluebrain.github.io/nexus/vocabulary/",
    "_"   -> iri"https://bluebrain.github.io/nexus/vocabulary/unconstrained.json"
  )

  def state(
      orgLabel: String,
      label: String,
      rev: Int,
      uuid: UUID = UUID.randomUUID(),
      orgUuid: UUID = UUID.randomUUID(),
      description: Option[String] = None,
      mappings: ApiMappings = ApiMappings.empty,
      base: Iri = nxv.base,
      vocab: Iri = nxv.base,
      deprecated: Boolean = false,
      markedForDeletion: Boolean = false,
      enforceSchema: Boolean = false,
      subject: Subject = Anonymous
  ): ProjectState =
    ProjectState(
      Label.unsafe(label),
      uuid,
      Label.unsafe(orgLabel),
      orgUuid,
      rev,
      deprecated,
      markedForDeletion,
      description,
      mappings,
      ProjectBase(base),
      vocab,
      enforceSchema,
      Instant.EPOCH,
      subject,
      Instant.EPOCH,
      subject
    )

  def project(
      orgLabel: String,
      label: String,
      uuid: UUID = UUID.randomUUID(),
      orgUuid: UUID = UUID.randomUUID(),
      description: Option[String] = None,
      mappings: ApiMappings = ApiMappings.empty,
      base: Iri = nxv.base,
      vocab: Iri = nxv.base,
      enforceSchema: Boolean = false,
      markedForDeletion: Boolean = false
  ): Project =
    Project(
      Label.unsafe(label),
      uuid,
      Label.unsafe(orgLabel),
      orgUuid,
      description,
      mappings,
      defaultApiMappings,
      ProjectBase(base),
      vocab,
      enforceSchema,
      markedForDeletion
    )

  def projectFields(project: Project): ProjectFields =
    ProjectFields(
      project.description,
      project.apiMappings,
      PrefixIri.unsafe(project.base.iri),
      PrefixIri.unsafe(project.vocab),
      project.enforceSchema
    )

  def resourceFor(
      project: Project,
      rev: Int = 1,
      subject: Subject = Anonymous,
      deprecated: Boolean = false,
      markedForDeletion: Boolean = false
  ): ProjectResource =
    state(
      project.organizationLabel.value,
      project.label.value,
      rev,
      project.uuid,
      project.organizationUuid,
      project.description,
      project.apiMappings,
      project.base.iri,
      project.vocab,
      deprecated,
      markedForDeletion,
      project.enforceSchema,
      subject
    ).toResource(defaultApiMappings)

}
