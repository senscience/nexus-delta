package ai.senscience.nexus.delta.sdk.directives

import ai.senscience.nexus.delta.kernel.utils.UrlUtils.encodeUriPath
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.projects.model.{ApiMappings, ProjectContext}
import ai.senscience.nexus.delta.sdk.utils.RouteHelpers
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.testkit.CirceLiteral
import ai.senscience.nexus.testkit.scalatest.TestMatchers
import ai.senscience.nexus.testkit.scalatest.ce.CatsEffectSpec
import akka.http.scaladsl.model.MediaRanges.`*/*`
import akka.http.scaladsl.model.headers.Accept
import akka.http.scaladsl.server.Directives.*
import akka.http.scaladsl.server.Route
import cats.effect.IO
import org.scalatest.{Inspectors, OptionValues}

class DeltaSchemeDirectivesSpec
    extends CatsEffectSpec
    with RouteHelpers
    with OptionValues
    with CirceLiteral
    with UriDirectives
    with TestMatchers
    with Inspectors {

  implicit private val baseUri: BaseUri = BaseUri.unsafe("http://localhost/base//", "v1")
  private val schemaView                = nxv + "schema"

  private val mappings = ApiMappings("alias" -> (nxv + "alias"), "nxv" -> nxv.base, "view" -> schemaView)
  private val vocab    = iri"http://localhost/vocab/"

  private val fetchContext = (_: ProjectRef) =>
    IO.pure(ProjectContext.unsafe(mappings, nxv.base, vocab, enforceSchema = false))

  private val schemeDirectives = new DeltaSchemeDirectives(fetchContext)

  private val route: Route =
    (get & uriPrefix(baseUri.base)) {
      import schemeDirectives.*
      concat(
        (pathPrefix("types") & projectRef & pathEndOrSingleSlash) { implicit projectRef =>
          types.apply { types =>
            val typesAsString = types.asRestrictedTo.map(_.iris.mkString(",")).getOrElse("")
            complete(typesAsString)
          }
        },
        baseUriPrefix(baseUri.prefix) {
          replaceUri("views", schemaView).apply {
            concat(
              (pathPrefix("views") & projectRef & idSegment & pathPrefix("other") & pathEndOrSingleSlash) {
                (project, id) =>
                  complete(s"project='$project',id='$id'")
              },
              pathPrefix("other") {
                complete("other")
              }
            )
          }
        }
      )
    }

  "A route" should {

    "return expanded types" in {
      Get("/base/types/org/proj?type=a&type=alias&type=nxv:rev") ~> Accept(`*/*`) ~> route ~> check {
        response.asString shouldEqual s"${nxv.rev.iri},${nxv + "alias"},http://localhost/vocab/a"
      }
    }

    "return a project and id when redirecting through the _ schema id" in {
      val endpoints = List("/base/v1/resources/org/proj/_/myid/other", "/base/v1/views/org/proj/myid/other")
      forAll(endpoints) { endpoint =>
        Get(endpoint) ~> Accept(`*/*`) ~> route ~> check {
          response.asString shouldEqual "project='org/proj',id='myid'"
        }
      }
    }

    "return a project and id when redirecting through the schema id" in {
      val encoded   = encodeUriPath(schemaView.toString)
      val endpoints =
        List("/base/v1/resources/org/proj/view/myid/other", s"/base/v1/resources/org/proj/$encoded/myid/other")
      forAll(endpoints) { endpoint =>
        Get(endpoint) ~> Accept(`*/*`) ~> route ~> check {
          response.asString shouldEqual "project='org/proj',id='myid'"
        }
      }
    }

    "reject when redirecting" in {
      Get("/base/v1/resources/org/proj/wrong_schema/myid/other") ~> Accept(`*/*`) ~> route ~> check {
        handled shouldEqual false
      }
    }

    "return the other route when redirecting is not being affected" in {
      Get("/base/v1/other") ~> Accept(`*/*`) ~> route ~> check {
        response.asString shouldEqual "other"
      }
    }
  }

}
