package ai.senscience.nexus.delta.plugins.blazegraph.routes

import ai.senscience.nexus.akka.marshalling.RdfMediaTypes.*
import ai.senscience.nexus.delta.plugins.blazegraph.client.{SparqlQueryResponse, SparqlQueryResponseType}
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.*
import ai.senscience.nexus.delta.sdk.marshalling.RdfRejectionHandler.*
import ai.senscience.nexus.delta.sdk.utils.HeadersUtils
import akka.http.scaladsl.model.MediaType
import akka.http.scaladsl.model.MediaTypes.`text/plain`
import akka.http.scaladsl.server.Directives.{extractRequest, provide}
import akka.http.scaladsl.server.{Directive, Directive1, Route}
import org.http4s.MediaType as Http4sMediaType

trait BlazegraphViewsDirectives {

  private val queryMediaTypes: Seq[MediaType.NonBinary] =
    List(
      `application/sparql-results+json`,
      `application/sparql-results+xml`,
      `application/ld+json`,
      `application/n-triples`,
      `text/plain`,
      `application/rdf+xml`
    )

  /**
    * Completes with ''UnacceptedResponseContentTypeRejection'' immediately (without rejecting)
    */
  private def emitUnacceptedMediaType(implicit
      cr: RemoteContextResolution,
      ordering: JsonKeyOrdering
  ): Route =
    discardEntityAndForceEmit(unacceptedMediaTypeRejection(queryMediaTypes))

  def queryResponseType(implicit
      cr: RemoteContextResolution,
      ordering: JsonKeyOrdering
  ): Directive1[SparqlQueryResponseType.Aux[SparqlQueryResponse]] =
    extractRequest.flatMap { req =>
      HeadersUtils.findFirst(req.headers, queryMediaTypes).flatMap { akkaMediaType =>
        val http4sMediaType = new Http4sMediaType(
          akkaMediaType.mainType,
          akkaMediaType.subType,
          akkaMediaType.isCompressible,
          akkaMediaType.isCompressible,
          fileExtensions = akkaMediaType.fileExtensions
        )
        SparqlQueryResponseType.fromMediaType(http4sMediaType)
      } match {
        case Some(responseType) => provide(responseType.asInstanceOf[SparqlQueryResponseType.Generic])
        case None               => Directive(_ => emitUnacceptedMediaType)
      }
    }
}
