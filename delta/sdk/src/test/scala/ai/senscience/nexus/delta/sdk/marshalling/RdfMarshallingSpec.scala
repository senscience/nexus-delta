package ai.senscience.nexus.delta.sdk.marshalling

import ai.senscience.nexus.akka.marshalling.RdfMediaTypes.*
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.rdf.jsonld.api.{JsonLdApi, TitaniumJsonLdApi}
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.SimpleResource
import ai.senscience.nexus.delta.sdk.SimpleResource.{context, contextIri}
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sdk.utils.RouteHelpers
import ai.senscience.nexus.testkit.CirceLiteral
import ai.senscience.nexus.testkit.scalatest.ce.CatsIOValues
import ai.senscience.nexus.testkit.scalatest.{BaseSpec, TestMatchers}
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.*
import org.scalatest.matchers.should.Matchers

import java.time.Instant

class RdfMarshallingSpec
    extends BaseSpec
    with RouteHelpers
    with Matchers
    with CirceLiteral
    with RdfMarshalling
    with CatsIOValues
    with TestMatchers {

  implicit private val api: JsonLdApi               = TitaniumJsonLdApi.strict
  implicit private val rcr: RemoteContextResolution = RemoteContextResolution.fixed(contextIri -> context)
  implicit private val ordering: JsonKeyOrdering    =
    JsonKeyOrdering.default(topKeys =
      List("@context", "@id", "@type", "reason", "details", "sourceId", "projectionId", "_total", "_results")
    )

  private val id       = nxv + "myresource"
  private val resource = SimpleResource(id, 1, Instant.EPOCH, "Maria", 20)

  "Converting JsonLd into an HttpResponse" should {
    val compacted = resource.toCompactedJsonLd.accepted
    val expanded  = resource.toExpandedJsonLd.accepted

    "succeed as compacted form" in {
      val response = Marshal(StatusCodes.OK -> compacted).to[HttpResponse].futureValue
      response.status shouldEqual StatusCodes.OK
      response.asJson shouldEqual compacted.json
      response.entity.contentType shouldEqual `application/ld+json`.toContentType
    }

    "succeed as expanded form" in {
      val response = Marshal(StatusCodes.OK -> expanded).to[HttpResponse].futureValue
      response.status shouldEqual StatusCodes.OK
      response.asJson shouldEqual expanded.json
      response.entity.contentType shouldEqual `application/ld+json`.toContentType
    }
  }

  "Converting Dot into an HttpResponse" should {
    val dot = resource.toDot.accepted

    "succeed" in {
      val response = Marshal(StatusCodes.OK -> dot).to[HttpResponse].futureValue
      response.status shouldEqual StatusCodes.OK
      response.asString should equalLinesUnordered(dot.value)
      response.entity.contentType shouldEqual `text/vnd.graphviz`.toContentType
    }
  }

  "Converting NTriples into an HttpResponse" should {
    val ntriples = resource.toNTriples.accepted

    "succeed" in {
      val response = Marshal(StatusCodes.OK -> ntriples).to[HttpResponse].futureValue
      response.status shouldEqual StatusCodes.OK
      response.asString should equalLinesUnordered(ntriples.value)
      response.entity.contentType shouldEqual `application/n-triples`.toContentType
    }
  }

  "Converting NQuads into an HttpResponse" should {
    val nquads = resource.toNQuads.accepted

    "succeed" in {
      val response = Marshal(StatusCodes.OK -> nquads).to[HttpResponse].futureValue
      response.status shouldEqual StatusCodes.OK
      response.asString should equalLinesUnordered(nquads.value)
      response.entity.contentType shouldEqual `application/n-quads`.toContentType
    }
  }

}
