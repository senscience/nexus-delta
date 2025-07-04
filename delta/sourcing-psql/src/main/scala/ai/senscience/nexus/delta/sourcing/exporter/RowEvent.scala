package ai.senscience.nexus.delta.sourcing.exporter

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sourcing.model.{EntityType, Label}
import ai.senscience.nexus.delta.sourcing.offset.Offset
import doobie.Read
import io.circe.{Codec, Decoder, Encoder, Json}

import java.time.Instant

final case class RowEvent(
    ordering: Offset.At,
    `type`: EntityType,
    org: Label,
    project: Label,
    id: Iri,
    rev: Int,
    value: Json,
    instant: Instant
)

object RowEvent {

  implicit final val inputEventEncoder: Codec[RowEvent] = {
    import io.circe.generic.extras.Configuration
    import io.circe.generic.extras.semiauto.deriveConfiguredCodec
    implicit val offsetEncoder: Encoder[Offset.At] = Encoder.encodeLong.contramap(_.value)
    implicit val offsetDecoder: Decoder[Offset.At] = Decoder.decodeLong.map(Offset.At)
    implicit val config: Configuration             = Configuration.default
    deriveConfiguredCodec[RowEvent]
  }

  implicit val inputEventRead: Read[RowEvent] = {
    import ai.senscience.nexus.delta.sourcing.implicits.*
    import doobie.*
    import doobie.postgres.implicits.*
    Read[(Long, EntityType, Label, Label, Iri, Int, Json, Instant)].map {
      case (offset, entityType, org, project, id, rev, value, instant) =>
        RowEvent(Offset.At(offset), entityType, org, project, id, rev, value, instant)
    }
  }
}
