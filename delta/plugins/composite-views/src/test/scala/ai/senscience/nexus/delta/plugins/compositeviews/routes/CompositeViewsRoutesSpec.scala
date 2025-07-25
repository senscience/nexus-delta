package ai.senscience.nexus.delta.plugins.compositeviews.routes

import ai.senscience.nexus.akka.marshalling.RdfMediaTypes.`application/sparql-query`
import ai.senscience.nexus.akka.marshalling.{CirceMarshalling, RdfMediaTypes}
import ai.senscience.nexus.delta.kernel.utils.UrlUtils.{encodeUriPath, encodeUriQuery}
import ai.senscience.nexus.delta.plugins.blazegraph.client.SparqlQueryClientDummy
import ai.senscience.nexus.delta.plugins.compositeviews.CompositeViews
import ai.senscience.nexus.delta.plugins.compositeviews.model.permissions
import ai.senscience.nexus.delta.rdf.IriOrBNode.{BNode, Iri}
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.rdf.graph.NTriples
import ai.senscience.nexus.delta.rdf.query.SparqlQuery
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress
import ai.senscience.nexus.delta.sdk.directives.DeltaSchemeDirectives
import ai.senscience.nexus.delta.sdk.fusion.FusionConfig
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sdk.model.{IdSegment, ResourceAccess}
import ai.senscience.nexus.delta.sdk.projects.FetchContextDummy
import ai.senscience.nexus.delta.sdk.resolvers.ResolverContextResolution
import ai.senscience.nexus.delta.sdk.views.CompositeViewErrors.{viewIsDeprecatedError, viewIsNotDeprecatedError}
import akka.http.scaladsl.model.MediaTypes.`text/html`
import akka.http.scaladsl.model.headers.{`Content-Type`, Accept, Location}
import akka.http.scaladsl.model.{HttpEntity, StatusCodes, Uri}
import akka.http.scaladsl.server.Route
import akka.util.ByteString
import io.circe.syntax.*
import org.scalatest.Assertion

import scala.concurrent.duration.*

class CompositeViewsRoutesSpec extends CompositeViewsRoutesFixtures {

  implicit private val f: FusionConfig = fusionConfig

  private val viewId  = nxv + uuid.toString
  private val esId    = iri"http://example.com/es-projection"
  private val blazeId = iri"http://example.com/blazegraph-projection"

  private val selectQuery = SparqlQuery("SELECT * WHERE {?s ?p ?o}")
  private val esQuery     = jobj"""{"query": {"match_all": {} } }"""
  private val esResult    = json"""{"k": "v"}"""

  private val responseCommonNs         = NTriples("queryCommonNs", BNode.random)
  private val responseQueryProjection  = NTriples("queryProjection", BNode.random)
  private val responseQueryProjections = NTriples("queryProjections", BNode.random)

  private val fetchContext    = FetchContextDummy(List(project))
  private val groupDirectives = DeltaSchemeDirectives(fetchContext)

  private lazy val views: CompositeViews = CompositeViews(
    fetchContext,
    ResolverContextResolution(rcr),
    alwaysValidate,
    1.minute,
    eventLogConfig,
    xas,
    clock
  ).accepted

  private lazy val blazegraphQuery = new BlazegraphQueryDummy(
    new SparqlQueryClientDummy(
      sparqlNTriples = {
        case seq if seq.toSet == Set("queryCommonNs")    => responseCommonNs
        case seq if seq.toSet == Set("queryProjection")  => responseQueryProjection
        case seq if seq.toSet == Set("queryProjections") => responseQueryProjections
        case _                                           => NTriples.empty
      }
    ),
    views
  )

  private lazy val elasticSearchQuery =
    new ElasticSearchQueryDummy(Map((esId: IdSegment, esQuery) -> esResult), Map(esQuery -> esResult), views)

  private lazy val routes =
    Route.seal(
      CompositeViewsRoutesHandler(
        groupDirectives,
        CompositeViewsRoutes(
          identities,
          aclCheck,
          views,
          blazegraphQuery,
          elasticSearchQuery
        )
      )
    )

  val viewSource        = jsonContentOf("composite-view-source.json")
  val viewSourceUpdated = jsonContentOf("composite-view-source-updated.json")

  override def beforeAll(): Unit = {
    super.beforeAll()
    aclCheck.append(AclAddress.Root, reader -> Set(permissions.read)).accepted
    aclCheck.append(AclAddress.Root, writer -> Set(permissions.write)).accepted
  }

  "Composite views routes" should {
    "fail to create a view without permission" in {
      Post("/v1/views/myorg/myproj", viewSource.toEntity) ~> routes ~> check {
        response.shouldBeForbidden
      }
    }

    "create a view" in {
      Post("/v1/views/myorg/myproj", viewSource.toEntity) ~> as(writer) ~> routes ~> check {
        response.status shouldEqual StatusCodes.Created
        response.asJson shouldEqual viewMetadata(1, false)
      }
    }

    "reject creation of a view which already exists" in {
      Put(s"/v1/views/myorg/myproj/$uuid", viewSource.toEntity) ~> as(writer) ~> routes ~> check {
        response.status shouldEqual StatusCodes.Conflict
        response.asJson shouldEqual jsonContentOf("routes/errors/view-already-exists.json", "uuid" -> uuid)
      }
    }

    "fail to update a view without permission" in {
      Put(s"/v1/views/myorg/myproj/$uuid?rev=1", viewSource.toEntity) ~> as(reader) ~> routes ~> check {
        response.shouldBeForbidden
      }
    }

    "update a view" in {
      Put(s"/v1/views/myorg/myproj/$uuid?rev=1", viewSourceUpdated.toEntity) ~> as(writer) ~> routes ~> check {
        response.status shouldEqual StatusCodes.OK
        response.asJson shouldEqual viewMetadata(2, false)
      }
    }

    "reject update of a view at a non-existent revision" in {
      Put(s"/v1/views/myorg/myproj/$uuid?rev=3", viewSourceUpdated.toEntity) ~> as(writer) ~> routes ~> check {
        response.status shouldEqual StatusCodes.Conflict
        response.asJson shouldEqual jsonContentOf("routes/errors/incorrect-rev.json", "provided" -> 3, "expected" -> 2)
      }
    }

    "fail to fetch a view without permission" in {
      Get(s"/v1/views/myorg/myproj/$uuid") ~> routes ~> check {
        response.shouldBeForbidden
      }
    }

    "fetch a view" in {
      Get(s"/v1/views/myorg/myproj/$uuid") ~> as(reader) ~> routes ~> check {
        response.status shouldEqual StatusCodes.OK
        response.asJson should equalIgnoreArrayOrder(view(2, false, "2 minutes"))
      }
    }

    "fetch a view by rev" in {
      val endpoints = List(
        s"/v1/views/myorg/myproj/$uuid?rev=1",
        s"/v1/resources/myorg/myproj/_/$uuid?rev=1"
      )
      forAll(endpoints) { endpoint =>
        Get(endpoint) ~> as(reader) ~> routes ~> check {
          response.status shouldEqual StatusCodes.OK
          response.asJson should equalIgnoreArrayOrder(
            view(1, false, "1 minute")
              .mapObject(_.remove("resourceTag"))
          )
        }
      }
    }

    "fetch a view source" in {
      val endpoints = List(s"/v1/views/myorg/myproj/$uuid/source", s"/v1/resources/myorg/myproj/_/$uuid/source")
      forAll(endpoints) { endpoint =>
        Get(endpoint) ~> as(reader) ~> routes ~> check {
          response.status shouldEqual StatusCodes.OK
          response.asJson shouldEqual viewSourceUpdated.removeAllKeys("token")
        }
      }
    }

    "reject if provided rev and tag simultaneously" in {
      Get(s"/v1/views/myorg/myproj/$uuid?rev=1&tag=mytag") ~> as(reader) ~> routes ~> check {
        status shouldEqual StatusCodes.BadRequest
        response.asJson shouldEqual jsonContentOf("routes/errors/tag-and-rev-error.json")
      }
    }

    "query blazegraph common namespace and projection(s)" in {
      val encodedId = encodeUriPath(blazeId.toString)
      val mediaType = RdfMediaTypes.`application/n-triples`

      val queryEntity = HttpEntity(`application/sparql-query`, ByteString(selectQuery.value))
      val accept      = Accept(mediaType)
      val list        = List(
        s"/v1/views/myorg/myproj/$uuid/sparql"                        -> responseCommonNs.value,
        s"/v1/views/myorg/myproj/$uuid/projections/_/sparql"          -> responseQueryProjections.value,
        s"/v1/views/myorg/myproj/$uuid/projections/$encodedId/sparql" -> responseQueryProjection.value
      )

      forAll(list) { case (endpoint, expected) =>
        val postRequest = Post(endpoint, queryEntity).withHeaders(accept)
        val getRequest  = Get(s"$endpoint?query=${encodeUriQuery(selectQuery.value)}").withHeaders(accept)
        forAll(List(postRequest, getRequest)) { req =>
          req ~> routes ~> check {
            response.status shouldEqual StatusCodes.OK
            response.header[`Content-Type`].value.value shouldEqual mediaType.value
            response.asString shouldEqual expected
          }
        }
      }
    }

    "query elasticsearch projection(s)" in {
      val encodedId = encodeUriPath(esId.toString)

      val endpoints = List(
        s"/v1/views/myorg/myproj/$uuid/projections/_/_search",
        s"/v1/views/myorg/myproj/$uuid/projections/$encodedId/_search"
      )

      forAll(endpoints) { endpoint =>
        Post(endpoint, esQuery.asJson)(CirceMarshalling.jsonMarshaller, ec) ~> routes ~> check {
          response.status shouldEqual StatusCodes.OK
          response.asJson shouldEqual esResult
        }
      }
    }

    "fail to deprecate a view without permission" in {
      Delete(s"/v1/views/myorg/myproj/$uuid?rev=3") ~> as(reader) ~> routes ~> check {
        response.shouldBeForbidden
      }
    }

    "reject a deprecation of a view without rev" in {
      Delete(s"/v1/views/myorg/myproj/$uuid") ~> as(writer) ~> routes ~> check {
        response.status shouldEqual StatusCodes.BadRequest
        response.asJson shouldEqual jsonContentOf("routes/errors/missing-query-param.json", "field" -> "rev")
      }
    }

    "deprecate a view" in {
      Delete(s"/v1/views/myorg/myproj/$uuid?rev=2") ~> as(writer) ~> routes ~> check {
        response.status shouldEqual StatusCodes.OK
        response.asJson shouldEqual viewMetadata(3, true)
      }
    }

    "fail to undeprecate a view without permission" in {
      givenADeprecatedView { view =>
        Put(s"/v1/views/myorg/myproj/$view/undeprecate?rev=1") ~> as(reader) ~> routes ~> check {
          response.shouldBeForbidden
        }
      }
    }

    "reject an undeprecation of a view without rev" in {
      givenADeprecatedView { view =>
        Put(s"/v1/views/myorg/myproj/$view/undeprecate") ~> as(writer) ~> routes ~> check {
          response.status shouldEqual StatusCodes.BadRequest
          response.asJson shouldEqual jsonContentOf("routes/errors/missing-query-param.json", "field" -> "rev")
        }
      }
    }

    "reject an undeprecation of a view that is not deprecated" in {
      givenAView { view =>
        Put(s"/v1/views/myorg/myproj/$view/undeprecate?rev=1") ~> as(writer) ~> routes ~> check {
          response.status shouldEqual StatusCodes.BadRequest
          response.asJson shouldEqual viewIsNotDeprecatedError(nxv + view)
        }
      }
    }

    "undeprecate a view" in {
      givenADeprecatedView { view =>
        Put(s"/v1/views/myorg/myproj/$view/undeprecate?rev=2") ~> as(writer) ~> routes ~> check {
          response.status shouldEqual StatusCodes.OK
          response.asJson shouldEqual viewMetadataWithId(nxv + view, 3, deprecated = false)
        }
      }
    }

    "reject querying blazegraph common namespace and projection(s) for a deprecated view" in {
      val encodedId = encodeUriPath(blazeId.toString)
      val mediaType = RdfMediaTypes.`application/sparql-results+json`

      val queryEntity = HttpEntity(`application/sparql-query`, ByteString(selectQuery.value))
      val accept      = Accept(mediaType)
      val list        = List(
        s"/v1/views/myorg/myproj/$uuid/sparql",
        s"/v1/views/myorg/myproj/$uuid/projections/_/sparql",
        s"/v1/views/myorg/myproj/$uuid/projections/$encodedId/sparql"
      )

      forAll(list) { endpoint =>
        val postRequest = Post(endpoint, queryEntity).withHeaders(accept)
        val getRequest  = Get(s"$endpoint?query=${encodeUriQuery(selectQuery.value)}").withHeaders(accept)
        forAll(List(postRequest, getRequest)) { req =>
          req ~> routes ~> check {
            response.status shouldEqual StatusCodes.BadRequest
            response.asJson shouldEqual viewIsDeprecatedError(viewId)
          }
        }
      }
    }

    "reject querying elasticsearch projection(s) for a deprecated view" in {
      val encodedId = encodeUriPath(esId.toString)

      val endpoints = List(
        s"/v1/views/myorg/myproj/$uuid/projections/_/_search",
        s"/v1/views/myorg/myproj/$uuid/projections/$encodedId/_search"
      )

      forAll(endpoints) { endpoint =>
        Post(endpoint, esQuery.asJson)(CirceMarshalling.jsonMarshaller, ec) ~> routes ~> check {
          response.status shouldEqual StatusCodes.BadRequest
          response.asJson shouldEqual viewIsDeprecatedError(viewId)
        }
      }
    }

    "redirect to fusion for the latest version if the Accept header is set to text/html" in {
      Get(s"/v1/views/myorg/myproj/$uuid") ~> Accept(`text/html`) ~> routes ~> check {
        response.status shouldEqual StatusCodes.SeeOther
        response.header[Location].value.uri shouldEqual Uri(
          s"https://bbp.epfl.ch/nexus/web/myorg/myproj/resources/$uuid"
        )
      }
    }
  }

  private def givenAView(test: String => Assertion): Assertion = {
    val viewId = genString()
    Put(s"/v1/views/myorg/myproj/$viewId", viewSource.toEntity) ~> as(writer) ~> routes ~> check {
      response.status shouldEqual StatusCodes.Created
    }
    test(viewId)
  }

  private def givenADeprecatedView(test: String => Assertion): Assertion = {
    givenAView { view =>
      Delete(s"/v1/views/myorg/myproj/$view?rev=1") ~> as(writer) ~> routes ~> check {
        response.status shouldEqual StatusCodes.OK
      }
      test(view)
    }
  }

  private def viewMetadata(rev: Int, deprecated: Boolean) =
    viewMetadataWithId(nxv + uuid.toString, rev, deprecated)

  private def viewMetadataWithId(id: Iri, rev: Int, deprecated: Boolean) =
    jsonContentOf(
      "routes/responses/view-metadata.json",
      "id"         -> id,
      "uuid"       -> uuid,
      "rev"        -> rev,
      "deprecated" -> deprecated,
      "self"       -> ResourceAccess("views", projectRef, id).uri
    )

  private def view(rev: Int, deprecated: Boolean, rebuildInterval: String) =
    jsonContentOf(
      "routes/responses/view.json",
      "uuid"            -> uuid,
      "deprecated"      -> deprecated,
      "rev"             -> rev,
      "rebuildInterval" -> rebuildInterval,
      "self"            -> ResourceAccess("views", projectRef, viewId).uri
    )
}
