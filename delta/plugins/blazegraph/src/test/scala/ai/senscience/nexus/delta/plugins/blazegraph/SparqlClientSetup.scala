package ai.senscience.nexus.delta.plugins.blazegraph

import ai.senscience.nexus.delta.plugins.blazegraph.client.{SparqlClient, SparqlTarget}
import ai.senscience.nexus.delta.plugins.blazegraph.client.SparqlTarget.{Blazegraph, Rdf4j}
import ai.senscience.nexus.delta.plugins.blazegraph.config.BlazegraphViewsConfig.OpentelemetryConfig
import ai.senscience.nexus.delta.plugins.blazegraph.config.SparqlAccess
import ai.senscience.nexus.delta.sdk.otel.OtelMetricsClient
import ai.senscience.nexus.testkit.blazegraph.BlazegraphContainer
import ai.senscience.nexus.testkit.rd4j.RDF4JContainer
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
  private val credentials   = None
  private val otelConfig    = OpentelemetryConfig(captureQueries = false)

  private def makeClient(target: SparqlTarget, endpoint: Uri) = {
    val access = SparqlAccess(NonEmptyVector.one(endpoint), target, credentials, queryTimeout, otelConfig)
    SparqlClient(access, metricsClient, "test")
  }

  def blazegraph(): Resource[IO, SparqlClient] =
    for {
      container <- BlazegraphContainer.resource()
      endpoint   = Uri.unsafeFromString(s"http://${container.getHost}:${container.getMappedPort(9999)}/blazegraph")
      client    <- makeClient(Blazegraph, endpoint)
    } yield client

  def rdf4j(): Resource[IO, SparqlClient] =
    for {
      container <- RDF4JContainer.resource()
      endpoint   = Uri.unsafeFromString(s"http://${container.getHost}:${container.getMappedPort(8080)}/rdf4j-server")
      client    <- makeClient(Rdf4j, endpoint)
    } yield client

  trait Fixture { self: CatsEffectSuite =>
    val blazegraphClient: IOFixture[SparqlClient] =
      ResourceSuiteLocalFixture("blazegraphClient", blazegraph())

    val rdf4jClient: IOFixture[SparqlClient] =
      ResourceSuiteLocalFixture("rdf4jClient", rdf4j())
  }

}
