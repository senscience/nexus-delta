package ai.senscience.nexus.delta.sdk.generators

import ai.senscience.nexus.delta.sdk.OrganizationResource
import ai.senscience.nexus.delta.sdk.organizations.model.{Organization, OrganizationState}
import ai.senscience.nexus.delta.sourcing.model.Identity.{Anonymous, Subject}
import ai.senscience.nexus.delta.sourcing.model.Label

import java.time.Instant
import java.util.UUID

object OrganizationGen {

  def state(
      label: String,
      rev: Int,
      uuid: UUID = UUID.randomUUID(),
      description: Option[String] = None,
      deprecated: Boolean = false,
      subject: Subject = Anonymous
  ): OrganizationState =
    OrganizationState(
      Label.unsafe(label),
      uuid,
      rev,
      deprecated,
      description,
      Instant.EPOCH,
      subject,
      Instant.EPOCH,
      subject
    )

  def organization(label: String, uuid: UUID = UUID.randomUUID(), description: Option[String] = None): Organization =
    Organization(Label.unsafe(label), uuid, description)

  def resourceFor(
      organization: Organization,
      rev: Int = 1,
      subject: Subject = Anonymous,
      deprecated: Boolean = false
  ): OrganizationResource =
    state(
      organization.label.value,
      rev,
      organization.uuid,
      organization.description,
      deprecated,
      subject
    ).toResource

}
