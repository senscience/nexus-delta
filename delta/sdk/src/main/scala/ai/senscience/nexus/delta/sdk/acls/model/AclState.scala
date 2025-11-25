package ai.senscience.nexus.delta.sdk.acls.model

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.{nxv, schemas}
import ai.senscience.nexus.delta.sdk.AclResource
import ai.senscience.nexus.delta.sdk.acls.Acls
import ai.senscience.nexus.delta.sdk.model.{ResourceAccess, ResourceF}
import ai.senscience.nexus.delta.sdk.permissions.model.Permission
import ai.senscience.nexus.delta.sourcing.Serializer
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.ResourceRef.Latest
import ai.senscience.nexus.delta.sourcing.model.{Identity, ResourceRef}
import ai.senscience.nexus.delta.sourcing.state.State.GlobalState
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}
import io.circe.syntax.EncoderOps
import io.circe.{Codec, Encoder}

import java.time.Instant

/**
  * An existing ACLs state.
  *
  * @param acl
  *   the Access Control List
  * @param rev
  *   the ACLs revision
  * @param createdAt
  *   the instant when the resource was created
  * @param createdBy
  *   the identity that created the resource
  * @param updatedAt
  *   the instant when the resource was last updated
  * @param updatedBy
  *   the identity that last updated the resource
  */
final case class AclState(
    acl: Acl,
    rev: Int,
    createdAt: Instant,
    createdBy: Subject,
    updatedAt: Instant,
    updatedBy: Subject
) extends GlobalState {

  /**
    * The relative [[Iri]] of the acl
    */
  override val id: Iri = Acls.encodeId(acl.address)

  /**
    * @return
    *   the current deprecation status (always false for acls)
    */
  def deprecated: Boolean = false

  /**
    * @return
    *   the schema reference that acls conforms to
    */
  def schema: ResourceRef = Latest(schemas.acls)

  /**
    * @return
    *   the collection of known types of acls resources
    */
  def types: Set[Iri] = Set(nxv.AccessControlList)

  def toResource: AclResource = {
    ResourceF(
      id = id,
      access = ResourceAccess.acl(acl.address),
      rev = rev,
      types = types,
      deprecated = deprecated,
      createdAt = createdAt,
      createdBy = createdBy,
      updatedAt = updatedAt,
      updatedBy = updatedBy,
      schema = schema,
      value = acl
    )
  }

}

object AclState {

  val serializer: Serializer[AclAddress, AclState] = {
    import Acl.Database.*
    import ai.senscience.nexus.delta.sourcing.model.Identity.Database.given
    given Configuration            = Serializer.circeConfiguration
    given Codec.AsObject[AclState] = Codec.AsObject.from(
      deriveConfiguredDecoder[AclState],
      Encoder.AsObject.instance { state =>
        deriveConfiguredEncoder[AclState].mapJsonObject(_.add("address", state.acl.address.asJson)).encodeObject(state)
      }
    )
    Serializer(Acls.encodeId)
  }

  def initial(permissions: Set[Permission]): AclState = AclState(
    Acl(AclAddress.Root, Identity.Anonymous -> permissions),
    0,
    Instant.EPOCH,
    Identity.Anonymous,
    Instant.EPOCH,
    Identity.Anonymous
  )

}
