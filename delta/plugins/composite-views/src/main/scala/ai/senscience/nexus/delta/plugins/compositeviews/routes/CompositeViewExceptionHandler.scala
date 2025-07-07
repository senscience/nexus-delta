package ai.senscience.nexus.delta.plugins.compositeviews.routes

import ai.senscience.nexus.delta.plugins.blazegraph.routes.BlazegraphExceptionHandler
import ai.senscience.nexus.delta.plugins.compositeviews.model.CompositeViewRejection
import ai.senscience.nexus.delta.plugins.compositeviews.model.CompositeViewRejection.ViewNotFound
import ai.senscience.nexus.delta.plugins.elasticsearch.routes.ElasticSearchExceptionHandler
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.discardEntityAndForceEmit
import ai.senscience.nexus.delta.sdk.directives.Response.Reject
import ai.senscience.nexus.delta.sdk.jsonld.JsonLdRejection
import ai.senscience.nexus.delta.sdk.jsonld.JsonLdRejection.{DecodingFailed, InvalidJsonLdFormat}
import akka.http.scaladsl.server.Directives.reject
import akka.http.scaladsl.server.ExceptionHandler

object CompositeViewExceptionHandler {

  private val rejectPredicate: Throwable => Boolean = {
    case _: ViewNotFound | _: DecodingFailed | _: InvalidJsonLdFormat => true
    case _                                                            => false
  }

  def apply(implicit cr: RemoteContextResolution, ordering: JsonKeyOrdering): ExceptionHandler = ExceptionHandler {
    case err: CompositeViewRejection if rejectPredicate(err) => reject(Reject(err))
    case err: CompositeViewRejection                         => discardEntityAndForceEmit(err)
    case err: JsonLdRejection if rejectPredicate(err)        => reject(Reject(err))
  }.withFallback(BlazegraphExceptionHandler.client)
    .withFallback(ElasticSearchExceptionHandler.client)
}
