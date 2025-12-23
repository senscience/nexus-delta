package ai.senscience.nexus.delta.sourcing.projections.model

import ai.senscience.nexus.delta.sourcing.offset.Offset
import cats.syntax.all.*
import io.circe.syntax.KeyOps
import io.circe.{Encoder, Json}

/**
  * The indexing status of a given resource
  */
enum IndexingStatus {

  /**
    * The resource still needs to be indexed (or reindexed for an update)
    */
  case Pending

  /**
    * The resource has been processed but has been discarded
    */
  case Discarded

  /**
    * The resource has been processed and is indexed
    */
  case Completed

}

object IndexingStatus {

  given Encoder[IndexingStatus] = Encoder.instance {
    case Pending   => Json.obj("status" := "Pending")
    case Discarded => Json.obj("status" := "Discarded")
    case Completed => Json.obj("status" := "Completed")
  }

  def fromOffsets(projectionOffset: Offset, resourceOffset: Offset): IndexingStatus =
    if resourceOffset > projectionOffset then IndexingStatus.Pending
    else IndexingStatus.Completed

}
