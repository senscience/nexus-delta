package ai.senscience.nexus.tests

import ai.senscience.nexus.pekko.marshalling.{CirceUnmarshalling, RdfMediaTypes}
import cats.effect.IO
import io.circe.optics.JsonPath.root
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.HttpMethods.GET
import org.apache.pekko.http.scaladsl.model.HttpRequest
import org.apache.pekko.http.scaladsl.model.headers.Accept
import org.scalatest.matchers.should.Matchers

class SparqlDsl(isBlazegraph: Boolean)(using as: ActorSystem) extends CirceUnmarshalling with Matchers {

  import as.dispatcher

  private val sparqlUrl      = if isBlazegraph then "http://localhost:9999" else "http://localhost:7070"
  private val listNamespaces =
    if isBlazegraph then "/blazegraph/namespace?describe-each-named-graph=false"
    else "/rdf4j-server/repositories"
  private val sparqlClient   = HttpClient(sparqlUrl)

  private def filterNamespaces =
    root.predicate.value.string.exist(_ == "http://www.bigdata.com/rdf#/features/KB/Namespace")

  def includes(namespaces: String*) =
    allNamespaces.map { all =>
      all should contain allElementsOf (namespaces)
    }

  def excludes(namespaces: String*) =
    allNamespaces.map { all =>
      all should not contain allElementsOf(namespaces)
    }

  def allNamespaces: IO[List[String]] = {
    sparqlClient(
      HttpRequest(
        method = GET,
        uri = s"$sparqlUrl$listNamespaces"
      ).addHeader(Accept(RdfMediaTypes.`application/sparql-results+json`))
    ).flatMap { res =>
      IO.fromFuture(IO(jsonUnmarshaller(res.entity)))
        .map { json =>
          if isBlazegraph then root.results.bindings.each.filter(filterNamespaces).`object`.value.string.getAll(json)
          else root.results.bindings.each.id.value.string.getAll(json)
        }
    }
  }

}
