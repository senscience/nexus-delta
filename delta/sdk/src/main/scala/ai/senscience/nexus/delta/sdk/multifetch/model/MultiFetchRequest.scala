package ai.senscience.nexus.delta.sdk.multifetch.model

import ai.senscience.nexus.delta.sdk.model.ResourceRepresentation
import ai.senscience.nexus.delta.sdk.multifetch.model.MultiFetchRequest.Input
import ai.senscience.nexus.delta.sourcing.model.{ProjectRef, ResourceRef}
import cats.data.NonEmptyList
import io.circe.Decoder

/**
  * Request to get multiple resources
  * @param format
  *   the output format for these resources
  * @param resources
  *   the list of resources
  */
final case class MultiFetchRequest(format: ResourceRepresentation, resources: NonEmptyList[Input]) {}

object MultiFetchRequest {

  def apply(representation: ResourceRepresentation, first: Input, others: Input*) =
    new MultiFetchRequest(representation, NonEmptyList.of(first, others*))

  final case class Input(id: ResourceRef, project: ProjectRef)

  implicit val multiFetchRequestDecoder: Decoder[MultiFetchRequest] = {
    import io.circe.generic.extras.Configuration
    import io.circe.generic.extras.semiauto.*
    implicit val cfg: Configuration = Configuration.default
    implicit val inputDecoder       = deriveConfiguredDecoder[Input]
    deriveConfiguredDecoder[MultiFetchRequest]
  }

}
