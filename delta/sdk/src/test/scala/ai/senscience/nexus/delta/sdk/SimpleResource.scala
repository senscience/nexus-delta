package ai.senscience.nexus.delta.sdk

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.sdk.marshalling.HttpResponseFields
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.testkit.CirceLiteral
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.{HttpHeader, StatusCode, StatusCodes}
import io.circe.syntax.*
import io.circe.{Encoder, JsonObject}

import java.time.Instant

final case class SimpleResource(id: Iri, rev: Int, createdAt: Instant, name: String, age: Int)

object SimpleResource extends CirceLiteral {

  val contextIri: Iri =
    iri"http://example.com/contexts/simple-resource.json"

  val context: ContextValue =
    json"""{ "@context": {"_rev": "${nxv + "rev"}", "_createdAt": "${nxv + "createdAt"}", "@vocab": "${nxv.base}"} }""".topContextValueOrEmpty

  implicit private val simpleResourceEncoder: Encoder.AsObject[SimpleResource] =
    Encoder.AsObject.instance { v =>
      JsonObject.empty
        .add("@id", v.id.asJson)
        .add("name", v.name.asJson)
        .add("age", v.name.asJson)
        .add("_rev", v.rev.asJson)
        .add("_createdAt", v.createdAt.asJson)
    }

  implicit val simpleResourceJsonLdEncoder: JsonLdEncoder[SimpleResource] =
    JsonLdEncoder.computeFromCirce(_.id, ContextValue(contextIri))

  implicit val simpleResourceHttpResponseFields: HttpResponseFields[SimpleResource] =
    new HttpResponseFields[SimpleResource] {
      override def statusFrom(value: SimpleResource): StatusCode = StatusCodes.Accepted

      override def headersFrom(value: SimpleResource): Seq[HttpHeader] = Seq(new RawHeader("Test", "Value"))

      override def entityTag(value: SimpleResource): Option[String] = Some(value.id.toString)
    }

  val rawHeader: RawHeader = new RawHeader("Test", "Value")

}
