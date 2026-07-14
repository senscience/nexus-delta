package ai.senscience.nexus.tests

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.pekko.marshalling.CirceUnmarshalling
import ai.senscience.nexus.testkit.CirceLiteral
import ai.senscience.nexus.tests.config.TestsConfig.ElasticsearchConfig
import cats.effect.IO
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.HttpMethods.{DELETE, GET}
import org.apache.pekko.http.scaladsl.model.headers.{Authorization, BasicHttpCredentials, RawHeader}
import org.apache.pekko.http.scaladsl.model.{HttpHeader, HttpRequest, StatusCode}
import org.apache.pekko.stream.Materializer
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers

import scala.concurrent.ExecutionContext

class ElasticsearchDsl(config: ElasticsearchConfig)(using
    as: ActorSystem,
    materializer: Materializer,
    ec: ExecutionContext
) extends CirceLiteral
    with CirceUnmarshalling
    with Matchers {

  private val logger = Logger[this.type]

  private val elasticClient = HttpClient(config.base)

  // Serverless clusters authenticate with an API key while a standard cluster relies on basic auth.
  private val authHeader: HttpHeader =
    config.apiKey match {
      case Some(key) => RawHeader("Authorization", s"ApiKey $key")
      case None      => Authorization(BasicHttpCredentials(config.username, config.password))
    }

  def includes(indices: String*): IO[Assertion] =
    allIndices.map { all =>
      all should contain allElementsOf (indices)
    }

  def excludes(indices: String*): IO[Assertion] =
    allIndices.map { all =>
      all should not contain allElementsOf(indices)
    }

  def allIndices: IO[List[String]] = {
    // `_aliases` only lists indices carrying an alias on Elasticsearch Serverless, whereas Delta indices have none;
    // the resolve index API is used instead in that case as it lists every matching index.
    val uri =
      if config.serverless then s"${config.base}/_resolve/index/*"
      else s"${config.base}/_aliases"
    elasticClient(
      HttpRequest(method = GET, uri = uri).addHeader(authHeader)
    ).flatMap { res =>
      IO.fromFuture(IO(jsonUnmarshaller(res.entity)))
        .map { json =>
          if config.serverless then
            json.hcursor
              .downField("indices")
              .values
              .fold(List.empty[String])(_.flatMap(_.hcursor.get[String]("name").toOption).toList)
          else json.asObject.fold(List.empty[String])(_.keys.toList)
        }
    }
  }

  def deleteAllIndices(): IO[StatusCode] =
    elasticClient(
      HttpRequest(
        method = DELETE,
        uri = s"${config.base}/delta_*"
      ).addHeader(authHeader)
    ).onError { case t =>
      logger.error(t)("Error while deleting elasticsearch indices")
    }.flatMap { res =>
      logger.info(s"Deleting elasticsearch indices returned ${res.status}").as(res.status)
    }

}
