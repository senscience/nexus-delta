package ai.senscience.nexus.delta.sdk.model.source

import ai.senscience.nexus.delta.rdf.Vocabulary.contexts
import ai.senscience.nexus.delta.sdk.marshalling.HttpResponseFields
import ai.senscience.nexus.delta.sdk.model.{BaseUri, ResourceF}
import ai.senscience.nexus.delta.sdk.syntax.*
import cats.syntax.all.*
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}
import org.apache.pekko.http.scaladsl.model.{HttpHeader, StatusCode}

/**
  * Defines an original source (what has been provided by clients during the api call)
  *
  *   - We preserve all the provided values (i.e. even null values are preserved)
  */
sealed trait OriginalSource extends Product with Serializable {

  /**
    * The resource representation to annotate data and compute the conditional cache headers
    */
  def resourceF: ResourceF[Unit]

  /**
    * The original payload
    */
  def source: Json
}

object OriginalSource {

  /**
    * Standard original source
    *   - Only the payload provided by the user will be returned
    */
  final case class Standard(resourceF: ResourceF[Unit], source: Json) extends OriginalSource

  /**
    * Annotated original source
    *   - Injects alongside the original source, the metadata context (ex: audit values, @id, ...)
    */
  final case class Annotated(resourceF: ResourceF[Unit], source: Json) extends OriginalSource

  object Annotated {
    given BaseUri => Encoder[Annotated] =
      Encoder.instance { value =>
        val sourceWithoutMetadata = value.source.removeMetadataKeys()
        val metadataJson          = value.resourceF.asJson
        metadataJson.deepMerge(sourceWithoutMetadata).addContext(contexts.metadata)
      }
  }

  def apply[A](resourceF: ResourceF[A], source: Json, annotated: Boolean): OriginalSource =
    if annotated then Annotated(resourceF.void, source)
    else apply(resourceF, source)

  def apply[A](resourceF: ResourceF[A], source: Json): OriginalSource = Standard(resourceF.void, source)

  def annotated[A](resourceF: ResourceF[A], source: Json): OriginalSource =
    apply(resourceF, source, annotated = true)

  given BaseUri => Encoder[OriginalSource] =
    Encoder.instance {
      case standard: Standard =>
        standard.source
      case value: Annotated   =>
        value.asJson
    }

  given HttpResponseFields[OriginalSource] = {
    val resourceFHttpResponseField = ResourceF.resourceFHttpResponseFields[Unit]
    new HttpResponseFields[OriginalSource] {
      override def statusFrom(value: OriginalSource): StatusCode       =
        resourceFHttpResponseField.statusFrom(value.resourceF)
      override def headersFrom(value: OriginalSource): Seq[HttpHeader] =
        resourceFHttpResponseField.headersFrom(value.resourceF)
      override def entityTag(value: OriginalSource): Option[String]    =
        resourceFHttpResponseField.entityTag(value.resourceF)
    }
  }

}
