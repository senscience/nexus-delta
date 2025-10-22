package ai.senscience.nexus.delta.plugins.blazegraph.config

import ai.senscience.nexus.delta.plugins.blazegraph.client.SparqlTarget
import ai.senscience.nexus.delta.plugins.blazegraph.config.BlazegraphViewsConfig.OpentelemetryConfig
import ai.senscience.nexus.delta.sdk.instances.*
import cats.data.NonEmptyVector
import org.http4s.{BasicCredentials, Uri}
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader
import pureconfig.module.cats.*
import pureconfig.module.http4s.*

import scala.concurrent.duration.Duration

final case class SparqlAccess(
    endpoints: NonEmptyVector[Uri],
    target: SparqlTarget,
    credentials: Option[BasicCredentials],
    queryTimeout: Duration,
    otel: OpentelemetryConfig
)

object SparqlAccess {
  given ConfigReader[SparqlAccess] = deriveReader[SparqlAccess]
}
