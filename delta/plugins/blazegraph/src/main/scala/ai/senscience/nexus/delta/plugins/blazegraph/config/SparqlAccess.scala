package ai.senscience.nexus.delta.plugins.blazegraph.config

import ai.senscience.nexus.delta.kernel.http.client.middleware.HttpAuth
import ai.senscience.nexus.delta.plugins.blazegraph.client.SparqlTarget
import ai.senscience.nexus.delta.plugins.blazegraph.config.BlazegraphViewsConfig.OpentelemetryConfig
import cats.data.NonEmptyVector
import org.http4s.Uri
import pureconfig.ConfigReader
import pureconfig.error.CannotConvert
import pureconfig.generic.semiauto.deriveReader
import pureconfig.module.cats.*
import pureconfig.module.http4s.*

import scala.concurrent.duration.Duration

final case class SparqlAccess(
    endpoints: NonEmptyVector[Uri],
    target: SparqlTarget,
    credentials: HttpAuth,
    queryTimeout: Duration,
    otel: OpentelemetryConfig
)

object SparqlAccess {

  // Blazegraph is never serverless, so only basic and anonymous auth are accepted; api-key is rejected.
  given ConfigReader[SparqlAccess] =
    deriveReader[SparqlAccess].emap { access =>
      access.credentials match {
        case _: HttpAuth.ApiKey =>
          Left(
            CannotConvert(
              "api-key",
              "SparqlAccess",
              "Blazegraph does not support api-key authentication; use 'basic' or 'anonymous'"
            )
          )
        case _                  => Right(access)
      }
    }
}
