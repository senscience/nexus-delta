package ai.senscience.nexus.delta.sdk.generators

import ai.senscience.nexus.delta.sdk.AclResource
import ai.senscience.nexus.delta.sdk.acls.model.{Acl, AclState}
import ai.senscience.nexus.delta.sourcing.model.Identity
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject

import java.time.Instant

object AclGen {

  def state(
      acl: Acl,
      rev: Int,
      createdBy: Subject = Identity.Anonymous,
      updatedBy: Subject = Identity.Anonymous
  ): AclState =
    AclState(acl, rev, Instant.EPOCH, createdBy, Instant.EPOCH, updatedBy)

  def resourceFor(
      acl: Acl,
      rev: Int = 1,
      subject: Subject = Identity.Anonymous
  ): AclResource = state(acl, rev, subject, subject).toResource

}
