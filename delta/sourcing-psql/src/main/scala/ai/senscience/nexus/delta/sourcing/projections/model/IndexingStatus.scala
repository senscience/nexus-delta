package ai.senscience.nexus.delta.sourcing.projections.model

import ai.senscience.nexus.delta.sourcing.offset.Offset
import cats.syntax.all.*
import io.circe.syntax.KeyOps
import io.circe.{Encoder, Json}

sealed trait IndexingStatus extends Product with Serializable

object IndexingStatus {

  final case object Pending extends IndexingStatus

  final case object Discarded extends IndexingStatus

  final case object Completed extends IndexingStatus

  implicit val indexingStatusEncoder: Encoder[IndexingStatus] = Encoder.instance {
    case Pending   => Json.obj("status" := "Pending")
    case Discarded => Json.obj("status" := "Discarded")
    case Completed => Json.obj("status" := "Completed")
  }

  def fromOffsets(projectionOffset: Offset, resourceOffset: Offset): IndexingStatus =
    if (resourceOffset > projectionOffset)
      IndexingStatus.Pending
    else
      IndexingStatus.Completed

}
