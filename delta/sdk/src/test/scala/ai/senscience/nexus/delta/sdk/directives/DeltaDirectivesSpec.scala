package ai.senscience.nexus.delta.sdk.directives

import ai.senscience.nexus.akka.marshalling.CirceMarshalling
import ai.senscience.nexus.akka.marshalling.RdfMediaTypes.*
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.{contexts, nxv}
import ai.senscience.nexus.delta.rdf.jsonld.api.{JsonLdApi, TitaniumJsonLdApi}
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.SimpleResource
import ai.senscience.nexus.delta.sdk.SimpleResource.rawHeader
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.*
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectivesSpec.SimpleResource2
import ai.senscience.nexus.delta.sdk.fusion.FusionConfig
import ai.senscience.nexus.delta.sdk.marshalling.{RdfExceptionHandler, RdfRejectionHandler}
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.model.IdSegmentRef.{Latest, Revision, Tag}
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sdk.utils.RouteHelpers
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.model.Tag.UserTag
import ai.senscience.nexus.testkit.CirceLiteral
import ai.senscience.nexus.testkit.scalatest.BaseSpec
import ai.senscience.nexus.testkit.scalatest.ce.CatsEffectSpec
import akka.http.scaladsl.model.*
import akka.http.scaladsl.model.MediaRange.*
import akka.http.scaladsl.model.MediaRanges.{`*/*`, `application/*`, `audio/*`, `text/*`}
import akka.http.scaladsl.model.MediaTypes.{`application/json`, `text/html`, `text/plain`}
import akka.http.scaladsl.model.StatusCodes.*
import akka.http.scaladsl.model.headers.*
import akka.http.scaladsl.model.headers.HttpEncodings.gzip
import akka.http.scaladsl.server.Directives.*
import akka.http.scaladsl.server.{ExceptionHandler, RejectionHandler, Route}
import cats.effect.IO
import io.circe.syntax.*
import io.circe.{Encoder, JsonObject}
import org.scalatest.Inspectors

import java.time.Instant

class DeltaDirectivesSpec
    extends BaseSpec
    with RouteHelpers
    with CatsEffectSpec
    with CirceMarshalling
    with CirceLiteral
    with Inspectors {

  implicit private val ordering: JsonKeyOrdering =
    JsonKeyOrdering.default(topKeys =
      List("@context", "@id", "@type", "reason", "details", "sourceId", "projectionId", "_total", "_results")
    )

  implicit private val f: FusionConfig =
    FusionConfig(uri"https://bbp.epfl.ch/nexus/web/", enableRedirects = true, uri"https://bbp.epfl.ch")

  implicit val baseUri: BaseUri = BaseUri.unsafe("http://localhost", "v1")

  private val id                = nxv + "myresource"
  private val resource          = SimpleResource(id, 1, Instant.EPOCH, "Maria", 20)
  private val resourceFusionUri = Uri(
    "https://bbp.epfl.ch/nexus/web/org/proj/resources/https:%2F%2Fbluebrain.github.io%2Fnexus%2Fvocabulary%2Fid"
  )

  implicit val api: JsonLdApi = TitaniumJsonLdApi.strict

  implicit private val rcr: RemoteContextResolution =
    RemoteContextResolution.fixed(
      SimpleResource.contextIri -> SimpleResource.context,
      contexts.error            -> jsonContentOf("contexts/error.json").topContextValueOrEmpty
    )

  private val compacted = resource.toCompactedJsonLd.accepted

  private val ioResource = IO.pure(resource)

  private val redirectTarget = uri"http://localhost/test"
  private val ioRedirect     = IO.pure(redirectTarget)

  private val ref: ProjectRef  = ProjectRef.unsafe("org", "proj")
  private val ioProject        = IO.pure(ref.asJson)
  private val projectFusionUri = uri"https://bbp.epfl.ch/nexus/web/admin/org/proj"

  implicit val rejectionHandler: RejectionHandler = RdfRejectionHandler.apply
  implicit val exceptionHandler: ExceptionHandler = RdfExceptionHandler.apply

  private val routeUnsealed: Route =
    encodeResponse {
      get {
        concat(
          path("uio") {
            emit(Accepted, IO.pure(resource))
          },
          path("io") {
            emit(Accepted, ioResource)
          },
          path("value") {
            emit(resource)
          },
          path("fail") {
            emit(SimpleResource2(nxv + "myid", 1))
          },
          path("throw") {
            throw new IllegalArgumentException("")
          },
          path("redirectIO") {
            emitRedirect(StatusCodes.SeeOther, ioRedirect)
          },
          pathPrefix("resources") {
            concat(
              path("redirectFusionDisabled") {
                emitOrFusionRedirect(ref, Latest(nxv + "id"), emit(resource))(f.copy(enableRedirects = false))
              },
              path("redirectFusionLatest") {
                emitOrFusionRedirect(ref, Latest(nxv + "id"), emit(resource))
              },
              path("redirectFusionRev") {
                emitOrFusionRedirect(ref, Revision(nxv + "id", 7), emit(resource))
              },
              path("redirectFusionTag") {
                emitOrFusionRedirect(ref, Tag(nxv + "id", UserTag.unsafe("my-tag")), emit(resource))
              },
              path("redirectFusionDisabled") {
                emitOrFusionRedirect(ref, Latest(nxv + "id"), emit(resource))(f.copy(enableRedirects = false))
              }
            )
          },
          pathPrefix("projects") {
            concat(
              path("redirectFusionDisabled") {
                emitOrFusionRedirect(ref, emit(ioProject))(f.copy(enableRedirects = false))
              },
              path("redirectFusion") {
                emitOrFusionRedirect(ref, emit(ioProject))
              }
            )
          }
        )
      }
    }

  private val route: Route = Route.seal(routeUnsealed)

  "A route" should {

    "return the appropriate content type for Accept header that matches supported" in {
      val endpoints     = List("/uio", "/io")
      val acceptMapping = Map[Accept, MediaType](
        Accept(`*/*`)                                          -> `application/ld+json`,
        Accept(`application/ld+json`)                          -> `application/ld+json`,
        Accept(`application/json`)                             -> `application/json`,
        Accept(`application/json`, `application/ld+json`)      -> `application/json`,
        Accept(`application/ld+json`, `application/json`)      -> `application/ld+json`,
        Accept(`application/json`, `text/plain`)               -> `application/json`,
        Accept(`text/vnd.graphviz`, `application/json`)        -> `text/vnd.graphviz`,
        Accept(`application/*`)                                -> `application/ld+json`,
        Accept(`application/*`, `text/plain`)                  -> `application/ld+json`,
        Accept(`text/*`)                                       -> `text/vnd.graphviz`,
        Accept(`application/n-triples`, `application/ld+json`) -> `application/n-triples`,
        Accept(`text/plain`, `application/n-triples`)          -> `application/n-triples`,
        Accept(`application/n-quads`, `application/ld+json`)   -> `application/n-quads`,
        Accept(`text/plain`, `application/n-quads`)            -> `application/n-quads`
      )
      forAll(endpoints) { endpoint =>
        forAll(acceptMapping) { case (accept, mt) =>
          Get(endpoint) ~> accept ~> route ~> check {
            response.header[`Content-Type`].value.contentType.mediaType shouldEqual mt
          }
        }
      }
    }

    "return the application/ld+json for missing Accept header" in {
      val endpoints = List("/uio", "/io")
      forAll(endpoints) { endpoint =>
        Get(endpoint) ~> route ~> check {
          response.header[`Content-Type`].value.contentType.mediaType shouldEqual `application/ld+json`
        }
      }
    }

    "return payload in compacted JSON-LD format" in {

      val endpoints = List("/uio?format=compacted", "/uio", "/io?format=compacted", "/io")
      val accepted  = List(Accept(`*/*`), Accept(`application/*`, `application/ld+json`))

      forAll(endpoints) { endpoint =>
        forAll(accepted) { accept =>
          Get(endpoint) ~> accept ~> route ~> check {
            response.asJson shouldEqual compacted.json
            response.status shouldEqual Accepted
            response.headers.toList should contain(rawHeader)
          }
        }
      }
    }

    "return payload in expanded JSON-LD format" in {
      val expanded = resource.toExpandedJsonLd.accepted

      forAll(List(Accept(`*/*`), Accept(`application/*`, `application/ld+json`))) { accept =>
        Get("/uio?format=expanded") ~> accept ~> route ~> check {
          response.asJson shouldEqual expanded.json
          response.status shouldEqual Accepted
        }
      }

      forAll(List(Accept(`*/*`), Accept(`application/*`, `application/ld+json`))) { accept =>
        Get("/io?format=expanded") ~> accept ~> route ~> check {
          response.asJson shouldEqual expanded.json
          response.status shouldEqual Accepted
        }
      }
    }

    "return payload in Dot format" in {
      val dot = resource.toDot.accepted

      Get("/uio") ~> Accept(`text/vnd.graphviz`) ~> route ~> check {
        response.asString should equalLinesUnordered(dot.value)
        response.status shouldEqual Accepted
      }

      Get("/io") ~> Accept(`text/vnd.graphviz`) ~> route ~> check {
        response.asString should equalLinesUnordered(dot.value)
        response.status shouldEqual Accepted
      }
    }

    "return payload in NTriples format" in {
      val ntriples = resource.toNTriples.accepted

      Get("/uio") ~> Accept(`application/n-triples`) ~> route ~> check {
        response.asString should equalLinesUnordered(ntriples.value)
        response.status shouldEqual Accepted
      }

      Get("/io") ~> Accept(`application/n-triples`) ~> route ~> check {
        response.asString should equalLinesUnordered(ntriples.value)
        response.status shouldEqual Accepted
      }
    }

    "return payload in NQuads format" in {
      val nQuads = resource.toNQuads.accepted

      Get("/uio") ~> Accept(`application/n-quads`) ~> route ~> check {
        response.asString should equalLinesUnordered(nQuads.value)
        response.status shouldEqual Accepted
      }

      Get("/io") ~> Accept(`application/n-quads`) ~> route ~> check {
        response.asString should equalLinesUnordered(nQuads.value)
        response.status shouldEqual Accepted
      }
    }

    val expectedCompactedJsonLdTag        = EntityTag("9c006d4f8c6456808e399585b7fcf2ab")
    val expectedCompactedJsonLdGzippedTag = EntityTag("8b95a96128ad09c0cfafdd7874a40618")
    val expectedExpandedJsonLdTag         = EntityTag("353ccd87e58814e947c06e9590320ea1")

    "return the etag and last modified headers" in {
      Get("/io") ~> Accept(`application/ld+json`) ~> route ~> check {
        response.header[ETag].value shouldEqual ETag(expectedCompactedJsonLdTag)
      }
    }

    "return the etag and last modified headers accepting gzip" in {
      Get("/io") ~> Accept(`application/ld+json`) ~> `Accept-Encoding`(gzip) ~> route ~> check {
        response.header[ETag].value shouldEqual ETag(expectedCompactedJsonLdGzippedTag)
      }
    }

    "return another etag and last modified headers for the expanded format" in {
      Get("/io?format=expanded") ~> Accept(`application/ld+json`) ~> route ~> check {
        response.header[ETag].value shouldEqual ETag(expectedExpandedJsonLdTag)
      }
    }

    "return not modified when providing the etag" in {
      Get("/io") ~> Accept(`application/ld+json`) ~> `If-None-Match`(expectedCompactedJsonLdTag) ~> route ~> check {
        response.status shouldEqual NotModified
        response.asString shouldEqual ""
      }
    }

    "return the resource when providing an invalid tag computed without compression" in {
      // The provided etag was computed without gzip compression
      Get("/io") ~> Accept(`application/ld+json`) ~> `Accept-Encoding`(gzip) ~>
        `If-None-Match`(expectedCompactedJsonLdTag) ~> route ~> check {
          response.status shouldEqual Accepted
        }
    }

    "return the resource when providing a invalid tag" in {
      // The provided etag was computed without gzip compression
      Get("/io") ~> Accept(`application/ld+json`) ~>
        `If-None-Match`(expectedExpandedJsonLdTag) ~> route ~> check {
          response.status shouldEqual Accepted
        }
    }

    "handle RdfError using RdfExceptionHandler" in {
      Get("/fail") ~> Accept(`*/*`) ~> route ~> check {
        response.asJson shouldEqual jsonContentOf("directives/invalid-remote-context-error.json")
        response.status shouldEqual InternalServerError
      }
    }

    "handle unexpected exception using RdfExceptionHandler" in {
      Get("/throw") ~> Accept(`*/*`) ~> route ~> check {
        response.asJson shouldEqual jsonContentOf("directives/unexpected-error.json")
        response.status shouldEqual InternalServerError
      }
    }

    "reject when unaccepted Accept Header provided" in {
      forAll(List("/uio", "/io")) { endpoint =>
        Get(endpoint) ~> Accept(`audio/*`) ~> route ~> check {
          response.asJson shouldEqual jsonContentOf("directives/invalid-response-ct-rejection.json")
          response.status shouldEqual NotAcceptable
        }
      }
    }

    "reject when invalid query parameter provided" in {
      forAll(List("/uio?format=fake", "/io?format=fake")) { endpoint =>
        Get(endpoint) ~> Accept(`*/*`) ~> route ~> check {
          response.asJson shouldEqual jsonContentOf("directives/invalid-format-rejection.json")
          response.status shouldEqual NotFound
        }
      }
    }

    "redirect a successful io" in {
      Get("/redirectIO") ~> route ~> check {
        response.status shouldEqual StatusCodes.SeeOther
        val expected = Uri(redirectTarget.toString())
        response.header[Location].value.uri shouldEqual expected
      }
    }

    "not redirect to the resource fusion page if the feature is disabled" in {
      Get("/resources/redirectFusionDisabled") ~> Accept(`text/html`) ~> route ~> check {
        response.status shouldEqual StatusCodes.NotAcceptable
      }
    }

    "not redirect to the resource fusion page if the Accept header is not set to text/html" in {
      Get("/resources/redirectFusionLatest") ~> Accept(`application/json`) ~> route ~> check {
        response.asJson shouldEqual compacted.json
        response.status shouldEqual Accepted
      }
    }

    "redirect to the resource fusion page with the latest version if the Accept header is set to text/html" in {
      Get("/resources/redirectFusionLatest") ~> Accept(`text/html`) ~> route ~> check {
        response.status shouldEqual StatusCodes.SeeOther
        val expected = Uri(resourceFusionUri.toString())
        response.header[Location].value.uri shouldEqual expected
      }
    }

    "redirect to the resource fusion page with a fixed rev if the Accept header is set to text/html" in {
      Get("/resources/redirectFusionRev") ~> Accept(`text/html`) ~> route ~> check {
        response.status shouldEqual StatusCodes.SeeOther
        response.header[Location].value.uri shouldEqual resourceFusionUri.withQuery(Uri.Query("rev" -> "7"))
      }
    }

    "redirect to the resource fusion page with a given tag if the Accept header is set to text/html" in {
      Get("/resources/redirectFusionTag") ~> Accept(`text/html`) ~> route ~> check {
        response.status shouldEqual StatusCodes.SeeOther
        response.header[Location].value.uri shouldEqual resourceFusionUri.withQuery(Uri.Query("tag" -> "my-tag"))
      }
    }

    "not redirect to the project fusion page if the feature is disabled" in
      Get("/projects/redirectFusionDisabled") ~> Accept(`text/html`) ~> route ~> check {
        response.status shouldEqual StatusCodes.NotAcceptable
      }

    "not redirect to the project fusion page if the Accept header is not set to text/html" in
      Get("/projects/redirectFusion") ~> Accept(`application/json`) ~> route ~> check {
        response.asJson shouldEqual ref.asJson
        response.status shouldEqual OK
      }

    "redirect to the project fusion page with the latest version if the Accept header is set to text/html" in
      Get("/projects/redirectFusion") ~> Accept(`text/html`) ~> route ~> check {
        response.status shouldEqual StatusCodes.SeeOther
        val expected = Uri(projectFusionUri.toString())
        response.header[Location].value.uri shouldEqual expected
      }

  }

}

object DeltaDirectivesSpec {
  final case class SimpleResource2(id: Iri, rev: Int)

  object SimpleResource2 extends CirceLiteral {

    val contextIri: Iri = iri"http://example.com/contexts/simple-resource-2.json"

    implicit private val simpleResource2Encoder: Encoder.AsObject[SimpleResource2] =
      Encoder.AsObject.instance(v => JsonObject.empty.add("@id", v.id.asJson).add("_rev", v.rev.asJson))

    implicit val simpleResource2JsonLdEncoder: JsonLdEncoder[SimpleResource2] =
      JsonLdEncoder.computeFromCirce(_.id, ContextValue(contextIri))

  }
}
