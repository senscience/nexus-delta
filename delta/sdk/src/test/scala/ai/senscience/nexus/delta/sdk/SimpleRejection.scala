package ai.senscience.nexus.delta.sdk

import ai.senscience.nexus.delta.kernel.error.Rejection
import ai.senscience.nexus.delta.rdf.IriOrBNode.{BNode, Iri}
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.sdk.marshalling.HttpResponseFields
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.testkit.CirceLiteral
import akka.http.scaladsl.model.HttpMethods.GET
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.Allow
import io.circe.Encoder
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveConfiguredEncoder

sealed trait SimpleRejection extends Rejection {
  def reason: String
}

object SimpleRejection extends CirceLiteral {

  final case class BadRequestRejection(reason: String) extends SimpleRejection
  final case class ConflictRejection(reason: String)   extends SimpleRejection

  final val conflictRejection: SimpleRejection   = ConflictRejection("default conflict rejection")
  final val badRequestRejection: SimpleRejection = BadRequestRejection("default bad request rejection")

  val contextIri: Iri = iri"http://example.com/contexts/simple-rejection.json"

  val context: ContextValue = json"""{ "@context": {"@vocab": "${nxv.base}"} }""".topContextValueOrEmpty

  val bNode: BNode = BNode.random

  implicit private val cfg: Configuration =
    Configuration.default.withDiscriminator("@type")

  implicit private[sdk] val simpleRejectionEncoder: Encoder.AsObject[SimpleRejection] =
    deriveConfiguredEncoder[SimpleRejection]

  implicit val jsonLdEncoderSimpleRejection: JsonLdEncoder[SimpleRejection] =
    JsonLdEncoder.computeFromCirce(bNode, ContextValue(contextIri))

  implicit val statusFromSimpleRejection: HttpResponseFields[SimpleRejection] =
    HttpResponseFields.fromStatusAndHeaders {
      case _: BadRequestRejection => (StatusCodes.BadRequest, Seq(Allow(GET)))
      case _: ConflictRejection   => (StatusCodes.Conflict, Seq.empty)
    }
}
