package ai.senscience.nexus.delta.sdk.marshalling

import ai.senscience.nexus.delta.rdf.graph.{Dot, NQuads, NTriples}
import ai.senscience.nexus.delta.rdf.jsonld.JsonLd
import ai.senscience.nexus.delta.rdf.query.SparqlQuery
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.marshalling.RdfMarshalling.defaultWriterConfig
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.pekko.marshalling.RdfMediaTypes
import ai.senscience.nexus.pekko.marshalling.RdfMediaTypes.*
import com.github.plokhotnyuk.jsoniter_scala.circe.JsoniterScalaCodec
import com.github.plokhotnyuk.jsoniter_scala.core.*
import io.circe.{Json, Printer}
import org.apache.pekko.http.scaladsl.marshalling.{Marshaller, ToEntityMarshaller}
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.model.ContentTypes.`application/json`
import org.apache.pekko.http.scaladsl.model.MediaTypes.*
import org.apache.pekko.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, FromStringUnmarshaller, PredefinedFromEntityUnmarshallers, Unmarshaller}
import org.apache.pekko.util.ByteString

/**
  * Marshallings that allow pekko http to convert a type ''A'' to an [[HttpEntity]].
  */
trait RdfMarshalling {

  val defaultPrinter: Printer = Printer(dropNullValues = true, indent = "")
  val sourcePrinter: Printer  = Printer(dropNullValues = false, indent = "")

  private val ntriplesMediaTypes                        = List(`application/n-triples`, `text/plain`)
  val jsonMediaTypes: Seq[ContentType.WithFixedCharset] = List(`application/json`, `application/ld+json`.toContentType)

  /**
    * JsonLd -> HttpEntity
    */
  given jsonLdMarshaller: [A <: JsonLd] => (
      ordering: JsonKeyOrdering,
      codec: JsonValueCodec[Json] = RdfMarshalling.jsonCodecDropNull
  ) => ToEntityMarshaller[A] =
    Marshaller.withFixedContentType(ContentType(`application/ld+json`)) { jsonLd =>
      HttpEntity(
        `application/ld+json`,
        ByteString(writeToArray(jsonLd.json.sort, defaultWriterConfig))
      )
    }

  /**
    * Json -> HttpEntity
    */
  def customContentTypeJsonMarshaller(
      contentType: ContentType
  )(using
      ordering: JsonKeyOrdering,
      codec: JsonValueCodec[Json] = RdfMarshalling.jsonCodecDropNull
  ): ToEntityMarshaller[Json] =
    Marshaller.withFixedContentType(contentType) { json =>
      HttpEntity(
        contentType,
        ByteString(writeToArray(json.sort, defaultWriterConfig))
      )
    }

  /**
    * Json -> HttpEntity
    */
  given jsonMarshaller: (
      ordering: JsonKeyOrdering,
      codec: JsonValueCodec[Json] = RdfMarshalling.jsonCodecDropNull
  ) => ToEntityMarshaller[Json] =
    Marshaller.oneOf(jsonMediaTypes.map(customContentTypeJsonMarshaller)*)

  /**
    * NTriples -> HttpEntity
    */
  given nTriplesMarshaller: ToEntityMarshaller[NTriples] = {
    def inner(mediaType: MediaType.NonBinary): ToEntityMarshaller[NTriples] =
      Marshaller.StringMarshaller.wrap(mediaType)(_.value)

    Marshaller.oneOf(ntriplesMediaTypes.map(inner)*)
  }

  /**
    * NQuads -> HttpEntity
    */
  given nQuadsMarshaller: ToEntityMarshaller[NQuads] =
    Marshaller.StringMarshaller.wrap(`application/n-quads`)(_.value)

  /**
    * Dot -> HttpEntity
    */
  given dotMarshaller: ToEntityMarshaller[Dot] =
    Marshaller.StringMarshaller.wrap(`text/vnd.graphviz`)(_.value)

  given fromEntitySparqlQueryUnmarshaller: FromEntityUnmarshaller[SparqlQuery] =
    PredefinedFromEntityUnmarshallers.stringUnmarshaller
      .forContentTypes(RdfMediaTypes.`application/sparql-query`, MediaTypes.`text/plain`)
      .map(SparqlQuery(_))

  given fromStringSparqlQueryUnmarshaller: FromStringUnmarshaller[SparqlQuery] =
    Unmarshaller.strict(SparqlQuery(_))
}

object RdfMarshalling extends RdfMarshalling {
  private val defaultWriterConfig: WriterConfig = WriterConfig.withPreferredBufSize(100 * 1024)

  val jsonCodecDropNull: JsonValueCodec[Json] =
    JsoniterScalaCodec.jsonCodec(maxDepth = 512, doSerialize = _ ne Json.Null)
  val jsonSourceCodec: JsonValueCodec[Json]   = JsoniterScalaCodec.jsonCodec(maxDepth = 512)

}
