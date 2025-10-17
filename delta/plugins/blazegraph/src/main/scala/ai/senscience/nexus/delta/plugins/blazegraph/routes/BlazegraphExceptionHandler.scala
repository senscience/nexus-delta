package ai.senscience.nexus.delta.plugins.blazegraph.routes

import ai.senscience.nexus.delta.plugins.blazegraph.client.SparqlClientError
import ai.senscience.nexus.delta.plugins.blazegraph.model.BlazegraphViewRejection
import ai.senscience.nexus.delta.plugins.blazegraph.model.BlazegraphViewRejection.ViewNotFound
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.discardEntityAndForceEmit
import ai.senscience.nexus.delta.sdk.directives.Response.Reject
import ai.senscience.nexus.delta.sdk.jsonld.JsonLdRejection
import ai.senscience.nexus.delta.sdk.jsonld.JsonLdRejection.{DecodingFailed, InvalidJsonLdFormat}
import cats.effect.IO
import org.apache.pekko.http.scaladsl.server.Directives.reject
import org.apache.pekko.http.scaladsl.server.ExceptionHandler
import org.typelevel.otel4s.trace.Tracer

object BlazegraphExceptionHandler {

  private val rejectPredicate: Throwable => Boolean = {
    case _: ViewNotFound | _: DecodingFailed | _: InvalidJsonLdFormat => true
    case _                                                            => false
  }

  def apply(using RemoteContextResolution, JsonKeyOrdering, Tracer[IO]): ExceptionHandler = ExceptionHandler {
    case err: BlazegraphViewRejection if rejectPredicate(err) => reject(Reject(err))
    case err: BlazegraphViewRejection                         => discardEntityAndForceEmit(err)
    case err: JsonLdRejection if rejectPredicate(err)         => reject(Reject(err))
  }.withFallback(client)

  def client(using RemoteContextResolution, JsonKeyOrdering, Tracer[IO]): ExceptionHandler =
    ExceptionHandler { case err: SparqlClientError =>
      discardEntityAndForceEmit(err)
    }
}
