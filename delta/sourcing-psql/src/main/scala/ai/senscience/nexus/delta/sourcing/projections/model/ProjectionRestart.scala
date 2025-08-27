package ai.senscience.nexus.delta.sourcing.projections.model

import ai.senscience.nexus.delta.sourcing.Serializer
import ai.senscience.nexus.delta.sourcing.implicits.pgDecoderGetT
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.offset.Offset
import doobie.Get
import io.circe.Codec
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveConfiguredCodec

import java.time.Instant

/**
  * Intent to restart a given projection by a user
  * @param name
  *   the name of the projection to restart
  * @param fromOffset
  *   the offset to restart from
  * @param instant
  *   the instant the user performed the action
  * @param subject
  *   the user
  */
final case class ProjectionRestart(name: String, fromOffset: Offset, instant: Instant, subject: Subject)

object ProjectionRestart {

  implicit val projectionRestartCodec: Codec[ProjectionRestart] = {
    import ai.senscience.nexus.delta.sourcing.model.Identity.Database.*
    implicit val configuration: Configuration = Serializer.circeConfiguration
    deriveConfiguredCodec[ProjectionRestart]
  }

  implicit val projectionRestartGet: Get[ProjectionRestart] = pgDecoderGetT[ProjectionRestart]

}
