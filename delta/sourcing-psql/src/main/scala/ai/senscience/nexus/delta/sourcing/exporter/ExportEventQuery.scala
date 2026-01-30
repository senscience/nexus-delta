package ai.senscience.nexus.delta.sourcing.exporter

import ai.senscience.nexus.delta.sourcing.model.{Label, ProjectRef}
import ai.senscience.nexus.delta.sourcing.offset.Offset
import cats.data.NonEmptyList
import io.circe.Codec
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveConfiguredCodec

final case class ExportEventQuery(output: Label, projects: NonEmptyList[ProjectRef], offset: Offset)

object ExportEventQuery {

  private given Configuration   = Configuration.default.withStrictDecoding
  given Codec[ExportEventQuery] = deriveConfiguredCodec[ExportEventQuery]
}
