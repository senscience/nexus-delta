package ai.senscience.nexus.tests.kg

import ai.senscience.nexus.delta.kernel.utils.ClasspathResourceLoader
import ai.senscience.nexus.pekko.marshalling.CirceUnmarshalling
import ai.senscience.nexus.testkit.CirceLiteral
import ai.senscience.nexus.tests.{HttpClient, Identity}
import cats.effect.IO
import io.circe.Json
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.*

final class ElasticSearchViewsDsl(deltaClient: HttpClient) extends CirceUnmarshalling with CirceLiteral with Matchers {

  private val loader = ClasspathResourceLoader()

  /**
    * Create an aggregate view and expects it to succeed
    */
  def aggregate(id: String, projectRef: String, identity: Identity, views: (String, String)*): IO[Assertion] = {
    for {
      payload <- loader.jsonContentOf(
                   "kg/views/elasticsearch/aggregate.json",
                   "views" -> views.map { case ((project, view)) =>
                     Map(
                       "project" -> project,
                       "viewId"  -> view
                     ).asJava
                   }.asJava
                 )
      result  <- deltaClient.put[Json](s"/views/$projectRef/$id", payload, identity) { (_, response) =>
                   response.status shouldEqual StatusCodes.Created
                 }
    } yield result
  }

}
