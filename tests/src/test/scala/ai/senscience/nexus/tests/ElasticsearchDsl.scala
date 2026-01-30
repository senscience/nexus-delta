package ai.senscience.nexus.tests

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.pekko.marshalling.CirceUnmarshalling
import ai.senscience.nexus.testkit.CirceLiteral
import cats.effect.IO
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.HttpMethods.{DELETE, GET}
import org.apache.pekko.http.scaladsl.model.headers.BasicHttpCredentials
import org.apache.pekko.http.scaladsl.model.{HttpRequest, StatusCode}
import org.apache.pekko.stream.Materializer
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers

import scala.concurrent.ExecutionContext

class ElasticsearchDsl(using
    as: ActorSystem,
    materializer: Materializer,
    ec: ExecutionContext
) extends CirceLiteral
    with CirceUnmarshalling
    with Matchers {

  private val logger = Logger[this.type]

  private val elasticUrl    = s"http://${sys.props.getOrElse("elasticsearch-url", "localhost:9200")}"
  private val elasticClient = HttpClient(elasticUrl)
  private val credentials   = BasicHttpCredentials("elastic", "password")

  def includes(indices: String*): IO[Assertion] =
    allIndices.map { all =>
      all should contain allElementsOf (indices)
    }

  def excludes(indices: String*): IO[Assertion] =
    allIndices.map { all =>
      all should not contain allElementsOf(indices)
    }

  def allIndices: IO[List[String]] = {
    elasticClient(
      HttpRequest(
        method = GET,
        uri = s"$elasticUrl/_aliases"
      ).addCredentials(credentials)
    ).flatMap { res =>
      IO.fromFuture(IO(jsonUnmarshaller(res.entity)))
        .map(_.asObject.fold(List.empty[String])(_.keys.toList))
    }
  }

  def deleteAllIndices(): IO[StatusCode] =
    elasticClient(
      HttpRequest(
        method = DELETE,
        uri = s"$elasticUrl/delta_*"
      ).addCredentials(credentials)
    ).onError { case t =>
      logger.error(t)("Error while deleting elasticsearch indices")
    }.flatMap { res =>
      logger.info(s"Deleting elasticsearch indices returned ${res.status}").as(res.status)
    }

}
