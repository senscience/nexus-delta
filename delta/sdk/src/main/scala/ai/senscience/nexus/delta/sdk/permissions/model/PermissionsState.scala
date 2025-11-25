package ai.senscience.nexus.delta.sdk.permissions.model

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.{nxv, schemas}
import ai.senscience.nexus.delta.sdk.PermissionsResource
import ai.senscience.nexus.delta.sdk.model.{ResourceAccess, ResourceF}
import ai.senscience.nexus.delta.sdk.permissions.Permissions
import ai.senscience.nexus.delta.sourcing.Serializer
import ai.senscience.nexus.delta.sourcing.model.Identity.{Anonymous, Subject}
import ai.senscience.nexus.delta.sourcing.model.ResourceRef.Latest
import ai.senscience.nexus.delta.sourcing.model.{Label, ResourceRef}
import ai.senscience.nexus.delta.sourcing.state.State.GlobalState
import io.circe.Codec
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveConfiguredCodec

import java.time.Instant

/**
  * State for permissions
  */
final case class PermissionsState(
    rev: Int,
    permissions: Set[Permission],
    createdAt: Instant,
    createdBy: Subject,
    updatedAt: Instant,
    updatedBy: Subject
) extends GlobalState {

  /**
    * The relative [[Iri]] of the permission
    */
  override def id: Iri = Permissions.id

  override def deprecated: Boolean = false

  /**
    * @return
    *   the schema reference that permissions conforms to
    */
  def schema: ResourceRef = Latest(schemas.permissions)

  /**
    * @return
    *   the collection of known types of permissions resources
    */
  def types: Set[Iri] = Set(nxv.Permissions)

  def toResource(minimum: Set[Permission]): PermissionsResource = {
    ResourceF(
      id = id,
      access = ResourceAccess.permissions,
      rev = rev,
      types = types,
      deprecated = deprecated,
      createdAt = createdAt,
      createdBy = createdBy,
      updatedAt = updatedAt,
      updatedBy = updatedBy,
      schema = schema,
      value = PermissionSet(permissions ++ minimum)
    )
  }
}

object PermissionsState {

  def initial(minimum: Set[Permission]): PermissionsState = PermissionsState(
    0,
    minimum,
    Instant.EPOCH,
    Anonymous,
    Instant.EPOCH,
    Anonymous
  )

  val serializer: Serializer[Label, PermissionsState] = {
    import ai.senscience.nexus.delta.sourcing.model.Identity.Database.given
    given Configuration                    = Serializer.circeConfiguration
    given Codec.AsObject[PermissionsState] = deriveConfiguredCodec[PermissionsState]
    Serializer(_ => Permissions.id)
  }

}
