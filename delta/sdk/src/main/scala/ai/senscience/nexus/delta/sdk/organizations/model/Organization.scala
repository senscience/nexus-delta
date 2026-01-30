package ai.senscience.nexus.delta.sdk.organizations.model

import ai.senscience.nexus.delta.rdf.Vocabulary.{contexts, nxv}
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.sdk.OrderingFields
import ai.senscience.nexus.delta.sdk.organizations.model.Organization.Metadata
import ai.senscience.nexus.delta.sourcing.model.Label
import io.circe.Encoder
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveConfiguredEncoder

import java.util.UUID

/**
  * Representation of an organization.
  *
  * @param label
  *   the label of the organization
  * @param uuid
  *   the UUID of the organization
  * @param description
  *   an optional description of the organization
  */
final case class Organization(label: Label, uuid: UUID, description: Option[String]) {
  override def toString: String = label.toString

  /**
    * @return
    *   [[Organization]] metadata
    */
  def metadata: Metadata = Metadata(label, uuid)
}

object Organization {

  /**
    * Organization metadata.
    *
    * @param label
    *   the label of the organization
    * @param uuid
    *   the UUID of the organization
    */
  final case class Metadata(label: Label, uuid: UUID)

  private given Configuration = Configuration.default.copy(transformMemberNames = {
    case "label" => nxv.label.prefix
    case "uuid"  => nxv.uuid.prefix
    case other   => other
  })

  given Encoder.AsObject[Organization] = deriveConfiguredEncoder[Organization]

  val context: ContextValue         = ContextValue(contexts.organizations)
  given JsonLdEncoder[Organization] = JsonLdEncoder.computeFromCirce(context)

  private given Encoder.AsObject[Metadata] = deriveConfiguredEncoder[Metadata]
  given JsonLdEncoder[Metadata]            =
    JsonLdEncoder.computeFromCirce(ContextValue(contexts.organizationsMetadata))

  given OrderingFields[Organization] =
    OrderingFields {
      case "_label" => Ordering[String] on (_.label.value)
      case "_uuid"  => Ordering[UUID] on (_.uuid)
    }
}
