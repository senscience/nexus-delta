package ai.senscience.nexus.delta.elasticsearch.routes

import ai.senscience.nexus.delta.elasticsearch.model.ElasticSearchViewRejection
import ai.senscience.nexus.delta.elasticsearch.model.ElasticSearchViewRejection.ViewNotFound
import ai.senscience.nexus.delta.elasticsearch.query.ElasticSearchClientError
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.discardEntityAndForceEmit
import ai.senscience.nexus.delta.sdk.directives.Response.Reject
import ai.senscience.nexus.delta.sdk.jsonld.JsonLdRejection
import ai.senscience.nexus.delta.sdk.jsonld.JsonLdRejection.{DecodingFailed, InvalidJsonLdFormat}
import akka.http.scaladsl.server.Directives.reject
import akka.http.scaladsl.server.ExceptionHandler

object ElasticSearchExceptionHandler {

  private val rejectPredicate: Throwable => Boolean = {
    case _: ViewNotFound | _: DecodingFailed | _: InvalidJsonLdFormat => true
    case _                                                            => false
  }

  def apply(implicit cr: RemoteContextResolution, ordering: JsonKeyOrdering): ExceptionHandler = ExceptionHandler {
    case err: ElasticSearchViewRejection if rejectPredicate(err) => reject(Reject(err))
    case err: ElasticSearchViewRejection                         => discardEntityAndForceEmit(err)
    case err: JsonLdRejection if rejectPredicate(err)            => reject(Reject(err))
  }.withFallback(client)

  def client(implicit cr: RemoteContextResolution, ordering: JsonKeyOrdering): ExceptionHandler =
    ExceptionHandler { case err: ElasticSearchClientError =>
      discardEntityAndForceEmit(err)
    }

}
