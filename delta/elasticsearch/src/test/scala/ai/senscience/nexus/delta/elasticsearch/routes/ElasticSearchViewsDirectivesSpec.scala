package ai.senscience.nexus.delta.elasticsearch.routes

import ai.senscience.nexus.delta.elasticsearch.model.ResourcesSearchParams
import ai.senscience.nexus.delta.elasticsearch.model.ResourcesSearchParams.Type.{ExcludedType, IncludedType}
import ai.senscience.nexus.delta.kernel.search.TimeRange
import ai.senscience.nexus.delta.kernel.utils.UrlUtils.encodeUriQuery
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sdk.model.*
import ai.senscience.nexus.delta.sdk.model.search.{Sort, SortList}
import ai.senscience.nexus.delta.sdk.projects.model.{ApiMappings, ProjectContext}
import ai.senscience.nexus.delta.sdk.utils.RouteHelpers
import ai.senscience.nexus.delta.sourcing.model.Identity.User
import ai.senscience.nexus.delta.sourcing.model.Tag.UserTag
import ai.senscience.nexus.delta.sourcing.model.{Label, ResourceRef}
import ai.senscience.nexus.testkit.scalatest.ce.CatsEffectSpec
import akka.http.scaladsl.model.MediaRanges.`*/*`
import akka.http.scaladsl.model.headers.Accept
import akka.http.scaladsl.server.Directives.*
import akka.http.scaladsl.server.Route
import org.scalactic.source.Position

import java.time.Instant

class ElasticSearchViewsDirectivesSpec extends CatsEffectSpec with RouteHelpers with ElasticSearchViewsDirectives {

  implicit private val baseUri: BaseUri = BaseUri.unsafe("http://localhost", "v1")

  private val mappings                    = ApiMappings("alias" -> (nxv + "alias"), "nxv" -> nxv.base)
  private val base                        = iri"http://localhost/base/"
  private val vocab                       = iri"http://localhost/vocab/"
  implicit private val pc: ProjectContext = ProjectContext.unsafe(mappings, base, vocab, enforceSchema = false)

  private def makeRoute(
      expectedSortList: Option[SortList] = None,
      expectedSearch: Option[ResourcesSearchParams] = None
  )(implicit position: Position): Route =
    get {
      concat(
        (pathPrefix("sort") & sortList & pathEndOrSingleSlash) { list =>
          list shouldEqual expectedSortList.value
          complete("successSortList")
        },
        (pathPrefix("search") & projectRef & pathEndOrSingleSlash) { _ =>
          searchParameters(baseUri, pc).apply { params =>
            params shouldEqual expectedSearch.value
            complete("successSearchParams")
          }
        }
      )
    }

  "A route" should {

    "return the sort parameters" in {
      val expected = SortList(List(Sort("deprecated"), Sort("-@id"), Sort("_createdBy")))
      val route    = makeRoute(expectedSortList = Some(expected))
      Get("/sort?sort=+deprecated&sort=-@id&sort=_createdBy") ~> Accept(`*/*`) ~> route ~> check {
        responseAs[String] shouldEqual "successSortList"
      }
    }

    "return the search parameters" in {
      val alicia   = User("alicia", Label.unsafe("myrealm"))
      val aliciaId = encodeUriQuery(alicia.asIri.toString)
      val bob      = User("bob", Label.unsafe("myrealm"))
      val bobId    = encodeUriQuery(bob.asIri.toString)

      val createdAt        = TimeRange.Before(Instant.EPOCH)
      val createdAtEncoded = encodeUriQuery(s"*..${createdAt.value}")
      val updatedAt        = TimeRange.Between.unsafe(Instant.EPOCH, Instant.EPOCH.plusSeconds(5L))
      val updatedAtEncoded = encodeUriQuery(s"${updatedAt.start}..${updatedAt.end}")
      val tag              = UserTag.unsafe("mytag")

      val query = List(
        "locate"     -> "self",
        "id"         -> "myId",
        "deprecated" -> "false",
        "rev"        -> "2",
        "createdBy"  -> aliciaId,
        "createdAt"  -> createdAtEncoded,
        "updatedBy"  -> bobId,
        "updatedAt"  -> updatedAtEncoded,
        "rev"        -> "2",
        "type"       -> "A",
        "type"       -> "B",
        "type"       -> "-C",
        "schema"     -> "mySchema",
        "q"          -> "something",
        "tag"        -> tag.value
      ).map { case (k, v) => s"$k=$v" }.mkString("&")

      val expected = ResourcesSearchParams(
        locate = Some(iri"${base}self"),
        id = Some(iri"${base}myId"),
        deprecated = Some(false),
        rev = Some(2),
        createdBy = Some(alicia),
        createdAt = createdAt,
        updatedBy = Some(bob),
        updatedAt = updatedAt,
        types = List(
          IncludedType(iri"${vocab}A"),
          IncludedType(iri"${vocab}B"),
          ExcludedType(iri"${vocab}C")
        ),
        schema = Some(ResourceRef.Latest(iri"${base}mySchema")),
        q = Some("something"),
        tag = Some(tag)
      )

      val route = makeRoute(expectedSearch = Some(expected))

      Get(s"/search/org/project?$query") ~> Accept(`*/*`) ~> route ~> check {
        responseAs[String] shouldEqual "successSearchParams"
      }
    }

    "return empty search parameters" in {
      val expected = ResourcesSearchParams()
      val route    = makeRoute(expectedSearch = Some(expected))
      Get("/search/org/project") ~> Accept(`*/*`) ~> route ~> check {
        responseAs[String] shouldEqual "successSearchParams"
      }
    }
  }

}
