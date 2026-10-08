package ai.senscience.nexus.delta.plugins.blazegraph

import ai.senscience.nexus.delta.plugins.blazegraph.client.SparqlClient
import ai.senscience.nexus.delta.plugins.blazegraph.config.BlazegraphViewsConfig.OpentelemetryConfig
import ai.senscience.nexus.delta.plugins.blazegraph.config.SparqlAccess
import ai.senscience.nexus.delta.kernel.http.client.middleware.HttpAuth
import ai.senscience.nexus.delta.sdk.otel.OtelMetricsClient
import ai.senscience.nexus.testkit.blazegraph.BlazegraphContainer
import cats.data.NonEmptyVector
import cats.effect.{IO, Resource}
import munit.CatsEffectSuite
import munit.catseffect.IOFixture
import org.http4s.Uri
import org.typelevel.otel4s.trace.Tracer

import scala.concurrent.duration.*

object SparqlClientSetup extends Fixtures {

  private given Tracer[IO]  = Tracer.noop[IO]
  private val metricsClient = OtelMetricsClient.noop
  private val queryTimeout  = 10.seconds
  private val credentials   = HttpAuth.Anonymous
  private val otelConfig    = OpentelemetryConfig(captureQueries = false)

  def blazegraph(): Resource[IO, SparqlClient] =
    for {
      container <- BlazegraphContainer.resource()
      endpoint   = Uri.unsafeFromString(s"http://${container.getHost}:${container.getMappedPort(9999)}/blazegraph")
      access     = SparqlAccess(NonEmptyVector.one(endpoint), credentials, queryTimeout, otelConfig)
      client    <- SparqlClient(access, metricsClient, "test")
    } yield client

  trait Fixture { self: CatsEffectSuite =>
    val blazegraphClient: IOFixture[SparqlClient] =
      ResourceSuiteLocalFixture("blazegraphClient", blazegraph())
  }

}
