package ai.senscience.nexus.delta.sdk.utils

import ai.senscience.nexus.delta.sourcing.model.Identity.User
import io.circe.parser.parse
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Json, JsonObject, Printer}
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.model.HttpEntity.ChunkStreamPart
import org.apache.pekko.http.scaladsl.model.MediaTypes.`application/json`
import org.apache.pekko.http.scaladsl.model.headers.{ETag, OAuth2BearerToken}
import org.apache.pekko.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.testkit.TestDuration
import org.apache.pekko.util.ByteString
import org.scalactic.source.Position
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, Suite}

import java.nio.charset.StandardCharsets
import scala.concurrent.duration.*
import scala.reflect.ClassTag

trait RouteHelpers extends ScalatestRouteTest with ScalaFutures {
  self: Suite =>

  given RouteTestTimeout = RouteTestTimeout(6.seconds.dilated)

  private val consumer = new Consumer

  extension (http: HttpResponse) {

    def asString(using Materializer): String =
      consumer.asString(http.entity.dataBytes)

    def asJson(using Materializer): Json =
      consumer.asJson(http.entity.dataBytes)

    def asJsonObject(using Materializer): JsonObject = {
      val json = consumer.asJson(http.entity.dataBytes)
      json.asObject.getOrElse(
        fail(s"Error converting '$json' to a JsonObject.")
      )
    }

    def as[A: Decoder](using m: Materializer, A: ClassTag[A]): A =
      asJson.as[A] match {
        case Left(err)    =>
          fail(s"Error converting th json to '${A.runtimeClass.getName}'. Details: '${err.getMessage()}'")
        case Right(value) => value
      }

    def shouldBeForbidden(using Position): Assertion =
      shouldFail(StatusCodes.Forbidden, "AuthorizationFailed")

    def shouldFail(statusCode: StatusCode, errorType: String)(using Position): Assertion = {
      import consumer.*
      http.status shouldEqual statusCode
      asJsonObject(using materializer)("@type") shouldEqual Some(errorType.asJson)
    }

    def expectConditionalCacheHeaders(using Position): Assertion = {
      import consumer.*
      http.header[ETag] shouldBe defined
    }

    def expectNoConditionalCacheHeaders(using Position): Assertion = {
      import consumer.*
      http.header[ETag] shouldBe empty
    }
  }

  extension (chunks: Source[ChunkStreamPart, Any]) {
    def asString(entries: Long)(using Materializer): String =
      consumer.asString(chunks.map(chunk => chunk.data()), Some(entries))

    def asJson(entries: Long)(using Materializer): Json =
      consumer.asJson(chunks.map(chunk => chunk.data()), Some(entries))
  }

  extension (json: Json) {
    def toEntity(using printer: Printer = Printer.noSpaces.copy(dropNullValues = true)): RequestEntity =
      HttpEntity(`application/json`, ByteString(printer.printToByteBuffer(json, StandardCharsets.UTF_8)))
  }

  override given patienceConfig: PatienceConfig = PatienceConfig(6.seconds.dilated, 10.milliseconds)

  def as(user: User): RequestTransformer = addCredentials(OAuth2BearerToken(user.subject))
}

final class Consumer extends ScalaFutures with Matchers {

  private given PatienceConfig = PatienceConfig(6.seconds, 10.milliseconds)

  private def consume(source: Source[ByteString, Any])(using Materializer): String =
    source.runFold("")(_ ++ _.utf8String).futureValue

  private def consume(source: Source[ByteString, Any], entries: Long)(using Materializer): String =
    source.take(entries).runFold("")(_ ++ _.utf8String).futureValue

  def asString(source: Source[ByteString, Any], entries: Option[Long] = None)(using Materializer): String =
    entries.fold(consume(source))(consume(source, _))

  def asJson(source: Source[ByteString, Any], entries: Option[Long] = None)(using Materializer): Json = {
    val consumed = asString(source, entries)
    parse(consumed) match {
      case Left(err)    => fail(s"Error converting '$consumed' to Json. Details: '${err.getMessage()}'")
      case Right(value) => value
    }
  }
}
