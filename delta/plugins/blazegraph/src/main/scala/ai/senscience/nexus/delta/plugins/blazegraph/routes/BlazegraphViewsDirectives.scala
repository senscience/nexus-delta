package ai.senscience.nexus.delta.plugins.blazegraph.routes

import ai.senscience.nexus.delta.plugins.blazegraph.client.{SparqlQueryResponse, SparqlQueryResponseType}
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.*
import ai.senscience.nexus.delta.sdk.marshalling.RdfRejectionHandler.*
import ai.senscience.nexus.delta.sdk.utils.HeadersUtils
import ai.senscience.nexus.pekko.marshalling.RdfMediaTypes.*
import cats.effect.IO
import org.apache.pekko.http.scaladsl.model.MediaType
import org.apache.pekko.http.scaladsl.model.MediaTypes.`text/plain`
import org.apache.pekko.http.scaladsl.server.Directives.{extractRequest, provide}
import org.apache.pekko.http.scaladsl.server.{Directive, Directive1, Route}
import org.http4s.MediaType as Http4sMediaType
import org.typelevel.otel4s.trace.Tracer

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
  private def emitUnacceptedMediaType(using RemoteContextResolution, JsonKeyOrdering, Tracer[IO]): Route =
    discardEntityAndForceEmit(unacceptedMediaTypeRejection(queryMediaTypes))

  def queryResponseType(using
      RemoteContextResolution,
      JsonKeyOrdering,
      Tracer[IO]
  ): Directive1[SparqlQueryResponseType.Aux[SparqlQueryResponse]] =
    extractRequest.flatMap { req =>
      HeadersUtils.findFirst(req.headers, queryMediaTypes).flatMap { pekkoMediaType =>
        val http4sMediaType = new Http4sMediaType(
          pekkoMediaType.mainType,
          pekkoMediaType.subType,
          pekkoMediaType.isCompressible,
          pekkoMediaType.isCompressible,
          fileExtensions = pekkoMediaType.fileExtensions
        )
        SparqlQueryResponseType.fromMediaType(http4sMediaType)
      } match {
        case Some(responseType) => provide(responseType.asInstanceOf[SparqlQueryResponseType.Generic])
        case None               => Directive(_ => emitUnacceptedMediaType)
      }
    }
}
