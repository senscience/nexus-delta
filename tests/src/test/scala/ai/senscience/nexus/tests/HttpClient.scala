package ai.senscience.nexus.tests

import ai.senscience.nexus.delta.kernel.utils.UrlUtils.encodeUriPath
import ai.senscience.nexus.pekko.marshalling.{CirceUnmarshalling, RdfMediaTypes}
import ai.senscience.nexus.tests.GzipCompression.*
import ai.senscience.nexus.tests.HttpClient.{jsonHeaders, tokensMap}
import ai.senscience.nexus.tests.Identity.Anonymous
import ai.senscience.nexus.tests.kg.files.model.FileInput
import cats.effect.IO
import cats.effect.unsafe.implicits.*
import io.circe.Json
import io.circe.parser.*
import io.circe.syntax.*
import fs2.*
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.model.HttpMethods.*
import org.apache.pekko.http.scaladsl.model.Multipart.FormData
import org.apache.pekko.http.scaladsl.model.Multipart.FormData.BodyPart
import org.apache.pekko.http.scaladsl.model.headers.*
import org.apache.pekko.http.scaladsl.unmarshalling.FromEntityUnmarshaller
import org.apache.pekko.http.scaladsl.{Http, HttpExt}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.connectors.sse.scaladsl.EventSource
import org.apache.pekko.stream.scaladsl.Sink
import org.scalatest.matchers.should.Matchers
import org.scalatest.{AppendedClues, Assertion}

import java.nio.file.{Files, Path}
import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}

class HttpClient private (baseUrl: Uri, httpExt: HttpExt)(using
    as: ActorSystem,
    materializer: Materializer,
    ec: ExecutionContext
) extends Matchers
    with CirceUnmarshalling
    with AppendedClues {

  private def fromFuture[A](future: => Future[A]) = IO.fromFuture { IO.delay(future) }

  private def assertDeltaNodeHeader(response: HttpResponse) =
    response.headers.map(_.name()) should contain("X-Delta-Node").withClue("A default header is missing.")

  def apply(req: HttpRequest): IO[HttpResponse] =
    fromFuture(httpExt.singleRequest(req))

  def head(url: Uri, identity: Identity)(assertResponse: HttpResponse => Assertion): IO[Assertion] = {
    val req = HttpRequest(HEAD, s"$baseUrl$url", headers = identityHeader(identity).toList)
    fromFuture(httpExt.singleRequest(req)).map(assertResponse)
  }

  def run[A](req: HttpRequest)(using um: FromEntityUnmarshaller[A]): IO[(A, HttpResponse)] =
    fromFuture(httpExt.singleRequest(req)).flatMap { res =>
      fromFuture(um.apply(res.entity)).map(a => (a, res))
    }

  def post[A](url: String, body: Json, identity: Identity, extraHeaders: Seq[HttpHeader] = jsonHeaders)(
      assertResponse: (A, HttpResponse) => Assertion
  )(using um: FromEntityUnmarshaller[A]): IO[Assertion] =
    requestAssert(POST, url, Some(body), identity, extraHeaders)(assertResponse)

  def putJsonAndStatus(url: String, body: Json, identity: Identity): IO[(Json, StatusCode)] = {
    requestJsonAndStatus(PUT, url, Some(body), identity, jsonHeaders)
  }

  def put[A](url: String, body: Json, identity: Identity, extraHeaders: Seq[HttpHeader] = jsonHeaders)(
      assertResponse: (A, HttpResponse) => Assertion
  )(using um: FromEntityUnmarshaller[A]): IO[Assertion] =
    requestAssert(PUT, url, Some(body), identity, extraHeaders)(assertResponse)

  def postAndReturn[A](url: String, body: Json, identity: Identity, extraHeaders: Seq[HttpHeader] = jsonHeaders)(
      assertResponse: (A, HttpResponse) => Assertion
  )(using um: FromEntityUnmarshaller[A]): IO[A] =
    requestAssertAndReturn(POST, url, Some(body), identity, extraHeaders)(assertResponse).map(_._1)

  def putAndReturn[A](url: String, body: Json, identity: Identity, extraHeaders: Seq[HttpHeader] = jsonHeaders)(
      assertResponse: (A, HttpResponse) => Assertion
  )(using um: FromEntityUnmarshaller[A]): IO[A] =
    requestAssertAndReturn(PUT, url, Some(body), identity, extraHeaders)(assertResponse).map(_._1)

  /** Put with no body */
  def putEmptyBody[A](url: String, identity: Identity, extraHeaders: Seq[HttpHeader] = jsonHeaders)(
      assertResponse: (A, HttpResponse) => Assertion
  )(using um: FromEntityUnmarshaller[A]): IO[Assertion] =
    requestAssert(PUT, url, None, identity, extraHeaders)(assertResponse)

  def putAttachmentFromPath[A](
      url: String,
      path: Path,
      contentType: ContentType,
      fileName: String,
      identity: Identity,
      extraHeaders: Seq[HttpHeader] = jsonHeaders
  )(assertResponse: (A, HttpResponse) => Assertion)(using um: FromEntityUnmarshaller[A]): IO[Assertion] = {
    request(
      PUT,
      url,
      Some(path),
      identity,
      (p: Path) => {
        val entity = HttpEntity(contentType, Files.readAllBytes(p))
        FormData(BodyPart.Strict("file", entity, Map("filename" -> fileName))).toEntity
      },
      assertResponse,
      extraHeaders
    )
  }

  def uploadFile(project: String, storage: String, file: FileInput, rev: Option[Int])(
      assertResponse: (Json, HttpResponse) => Assertion
  )(using identity: Identity): IO[Assertion] =
    uploadFile(project, Some(storage), file, rev)(assertResponse)

  def uploadFile(project: String, storage: Option[String], file: FileInput, rev: Option[Int])(
      assertResponse: (Json, HttpResponse) => Assertion
  )(using identity: Identity): IO[Assertion] = {
    val storageParam                               = storage.map { s => s"storage=nxv:$s" }
    val revParam                                   = rev.map { r => s"&rev=$r" }
    val params                                     = (storageParam ++ revParam).mkString("?", "&", "")
    val requestPath                                = s"/files/$project/${encodeUriPath(file.fileId)}$params"
    def buildClue(a: Json, response: HttpResponse) =
      s"""
         |Endpoint: PUT $requestPath
         |Identity: $identity
         |Token: ${Option(tokensMap.get(identity)).map(_.credentials.token()).getOrElse("None")}
         |Status code: ${response.status}
         |Body: None
         |Response:
         |$a
         |""".stripMargin

    val metadataHeader          = file.metadata.map { m =>
      val json = Json.obj("name" := m.name, "description" := m.description, "keywords" := m.keywords)
      RawHeader("x-nxs-file-metadata", json.noSpaces)
    }
    val fileContentLengthHeader = RawHeader("x-nxs-file-content-length", file.contentLength.toString)

    request(
      PUT,
      requestPath,
      Some(file.contents),
      identity,
      (s: String) => {
        val entity = HttpEntity(file.contentType, s.getBytes)
        FormData(BodyPart.Strict("file", entity, Map("filename" -> file.filename))).toEntity
      },
      (a: Json, response: HttpResponse) => {
        assertDeltaNodeHeader(response)
        assertResponse(a, response).withClue(buildClue(a, response))
      },
      jsonHeaders ++ metadataHeader ++ Some(fileContentLengthHeader)
    )
  }

  def patch[A](url: String, body: Json, identity: Identity, extraHeaders: Seq[HttpHeader] = jsonHeaders)(
      assertResponse: (A, HttpResponse) => Assertion
  )(using um: FromEntityUnmarshaller[A]): IO[Assertion] =
    requestAssert(PATCH, url, Some(body), identity, extraHeaders)(assertResponse)

  def getWithBody[A](url: String, body: Json, identity: Identity, extraHeaders: Seq[HttpHeader] = jsonHeaders)(
      assertResponse: (A, HttpResponse) => Assertion
  )(using um: FromEntityUnmarshaller[A]): IO[Assertion] =
    requestAssert(GET, url, Some(body), identity, extraHeaders)(assertResponse)

  def get[A](url: String, identity: Identity, extraHeaders: Seq[HttpHeader] = jsonHeaders)(
      assertResponse: (A, HttpResponse) => Assertion
  )(using um: FromEntityUnmarshaller[A]): IO[Assertion] =
    requestAssert(GET, url, None, identity, extraHeaders)(assertResponse)

  def getJson[A](url: String, identity: Identity)(using um: FromEntityUnmarshaller[A]): IO[A] = {
    requestJson(GET, url, None, identity, (a: A, _: HttpResponse) => a, jsonHeaders)
  }

  def getResponse(url: String, identity: Identity, extraHeaders: Seq[HttpHeader] = jsonHeaders): IO[HttpResponse] =
    apply(
      HttpRequest(
        method = GET,
        uri = s"$baseUrl$url",
        headers = extraHeaders ++ identityHeader(identity)
      )
    )

  def getJsonAndStatus(url: String, identity: Identity): IO[(Json, StatusCode)] = {
    requestJsonAndStatus(GET, url, None, identity, jsonHeaders)
  }

  def deleteJsonAndStatus(url: String, identity: Identity): IO[(Json, StatusCode)] = {
    requestJsonAndStatus(DELETE, url, None, identity, jsonHeaders)
  }

  def deleteStatus(url: String, identity: Identity)(assertResponse: HttpResponse => Assertion): IO[Assertion] = {
    val req = HttpRequest(DELETE, s"$baseUrl$url", headers = identityHeader(identity).toList)
    fromFuture(httpExt.singleRequest(req)).map(assertResponse)
  }
  def delete[A](url: String, identity: Identity, extraHeaders: Seq[HttpHeader] = jsonHeaders)(
      assertResponse: (A, HttpResponse) => Assertion
  )(using um: FromEntityUnmarshaller[A]): IO[Assertion]                                                       =
    requestAssert(DELETE, url, None, identity, extraHeaders)(assertResponse)

  def requestAssertAndReturn[A](
      method: HttpMethod,
      url: String,
      body: Option[Json],
      identity: Identity,
      extraHeaders: Seq[HttpHeader] = jsonHeaders
  )(assertResponse: (A, HttpResponse) => Assertion)(using um: FromEntityUnmarshaller[A]): IO[(A, Assertion)] = {
    def buildClue(a: A, response: HttpResponse) =
      s"""
         |Endpoint: ${method.value} $url
         |Identity: $identity
         |Token: ${Option(tokensMap.get(identity)).map(_.credentials.token()).getOrElse("None")}
         |Status code: ${response.status}
         |Body: ${body.getOrElse("None")}
         |Response:
         |$a
         |""".stripMargin

    requestJson[A, (A, Assertion)](
      method,
      url,
      body,
      identity,
      (a: A, response: HttpResponse) => {
        assertDeltaNodeHeader(response)
        a -> assertResponse(a, response).withClue(buildClue(a, response))
      },
      extraHeaders
    )
  }

  def requestAssert[A](
      method: HttpMethod,
      url: String,
      body: Option[Json],
      identity: Identity,
      extraHeaders: Seq[HttpHeader] = jsonHeaders
  )(assertResponse: (A, HttpResponse) => Assertion)(using um: FromEntityUnmarshaller[A]): IO[Assertion] =
    requestAssertAndReturn[A](method, url, body, identity, extraHeaders) { (a, resp) =>
      assertResponse(a, resp)
    }.map(_._2)

  def sparqlQuery[A](url: String, query: String, identity: Identity, extraHeaders: Seq[HttpHeader] = Nil)(
      assertResponse: (A, HttpResponse) => Assertion
  )(using um: FromEntityUnmarshaller[A]): IO[Assertion] = {
    request(
      POST,
      url,
      Some(query),
      identity,
      (query: String) => HttpEntity(RdfMediaTypes.`application/sparql-query`, query),
      assertResponse,
      extraHeaders
    )
  }

  def requestJsonAndStatus(
      method: HttpMethod,
      url: String,
      body: Option[Json],
      identity: Identity,
      extraHeaders: Seq[HttpHeader]
  ): IO[(Json, StatusCode)] =
    request[Json, Json, (Json, StatusCode)](
      method,
      url,
      body,
      identity,
      (j: Json) => HttpEntity(ContentTypes.`application/json`, j.noSpaces),
      (json, response) => (json, response.status),
      extraHeaders
    )

  def requestJson[A, R](
      method: HttpMethod,
      url: String,
      body: Option[Json],
      identity: Identity,
      f: (A, HttpResponse) => R,
      extraHeaders: Seq[HttpHeader]
  )(using um: FromEntityUnmarshaller[A]): IO[R] = {
    def createEntity(json: Json) =
      if mustGzip(extraHeaders) then HttpEntity(ContentTypes.`application/json`, compress(json))
      else HttpEntity(ContentTypes.`application/json`, json.noSpaces)

    request(
      method,
      url,
      body,
      identity,
      (j: Json) => createEntity(j),
      f,
      extraHeaders
    )
  }

  private def identityHeader(identity: Identity): Option[HttpHeader] = {
    identity match {
      case Anonymous => None
      case _         =>
        Some(
          Option(tokensMap.get(identity)).getOrElse(
            throw new IllegalArgumentException(
              "The provided user has not been properly initialized, please add it to Identity.allUsers."
            )
          )
        )
    }
  }

  private val empty: RequestEntity              = HttpEntity.Empty
  def request[A, B, R](
      method: HttpMethod,
      url: String,
      body: Option[B],
      identity: Identity,
      toEntity: B => RequestEntity,
      f: (A, HttpResponse) => R,
      extraHeaders: Seq[HttpHeader]
  )(using um: FromEntityUnmarshaller[A]): IO[R] =
    apply(
      HttpRequest(
        method = method,
        uri = s"$baseUrl$url",
        headers = extraHeaders ++ identityHeader(identity),
        entity = body.fold(empty)(toEntity)
      )
    ).flatMap { res =>
      fromFuture { um(res.entity) }
        .map { f(_, res) }
    }

  def stream[A, B](
      url: String,
      nextUrl: A => Option[String],
      lens: A => B,
      identity: Identity,
      extraHeaders: Seq[HttpHeader] = jsonHeaders
  )(using um: FromEntityUnmarshaller[A]): Stream[IO, B] = {
    Stream.unfoldLoopEval[IO, String, B](url) { currentUrl =>
      requestJson[A, A](
        GET,
        currentUrl,
        None,
        identity,
        (a: A, _: HttpResponse) => a,
        extraHeaders
      ).map { a =>
        (lens(a), nextUrl(a))
      }
    }
  }

  def sseEvents(
      url: String,
      identity: Identity,
      initialLastEventId: Option[String],
      take: Long = 100L,
      takeWithin: FiniteDuration = 5.seconds
  )(assertResponse: Seq[(Option[String], Option[Json])] => Assertion): IO[Assertion] = {
    def send(request: HttpRequest): Future[HttpResponse] =
      apply(request.addHeader(tokensMap.get(identity))).unsafeToFuture()
    fromFuture {
      EventSource(s"$baseUrl$url", send, initialLastEventId = initialLastEventId)
        // drop resolver, views and storage events
        .take(take)
        .takeWithin(takeWithin)
        .runWith(Sink.seq)
    }
      .map { seq =>
        assertResponse(
          seq.map { s =>
            (s.eventType, parse(s.data).toOption)
          }
        )
      }
  }

}

object HttpClient {

  val tokensMap: ConcurrentHashMap[Identity, Authorization] = new ConcurrentHashMap[Identity, Authorization]

  val acceptAll: Seq[Accept] = Seq(Accept(MediaRanges.`*/*`))

  val acceptZip: Seq[Accept] = Seq(Accept(MediaTypes.`application/zip`, MediaTypes.`application/json`))

  val jsonHeaders: Seq[HttpHeader] = Accept(MediaTypes.`application/json`) :: Nil

  val gzipHeaders: Seq[HttpHeader] = Seq(Accept(MediaRanges.`*/*`), `Accept-Encoding`(HttpEncodings.gzip))

  def apply(baseUrl: Uri)(using ActorSystem, Materializer, ExecutionContext) = new HttpClient(baseUrl, Http())
}
