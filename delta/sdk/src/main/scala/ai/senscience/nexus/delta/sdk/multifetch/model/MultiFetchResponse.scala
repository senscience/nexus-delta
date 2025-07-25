package ai.senscience.nexus.delta.sdk.multifetch.model

import ai.senscience.nexus.delta.rdf.Vocabulary.contexts
import ai.senscience.nexus.delta.rdf.jsonld.api.{JsonLdApi, TitaniumJsonLdApi}
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.rdf.syntax.jsonLdEncoderSyntax
import ai.senscience.nexus.delta.sdk.jsonld.JsonLdContent
import ai.senscience.nexus.delta.sdk.marshalling.OriginalSource
import ai.senscience.nexus.delta.sdk.model.ResourceRepresentation.*
import ai.senscience.nexus.delta.sdk.model.{BaseUri, ResourceRepresentation}
import ai.senscience.nexus.delta.sdk.multifetch.model.MultiFetchResponse.Result
import ai.senscience.nexus.delta.sdk.multifetch.model.MultiFetchResponse.Result.itemEncoder
import ai.senscience.nexus.delta.sourcing.model.{ProjectRef, ResourceRef}
import cats.data.NonEmptyList
import cats.effect.IO
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json, JsonObject}

/**
  * A response for a multi-fetch operation
  * @param format
  *   the formats in which the resource should be represented
  * @param resources
  *   the result for each resource
  */
final case class MultiFetchResponse(format: ResourceRepresentation, resources: NonEmptyList[Result]) {

  /**
    * Encode the response as a Json payload
    */
  def asJson(implicit base: BaseUri, rcr: RemoteContextResolution): IO[Json] = {
    val encodeItem = itemEncoder(format)
    resources.traverse(encodeItem).map { r =>
      Json.obj(
        "format"    -> format.asJson,
        "resources" -> r.asJson
      )
    }
  }
}

object MultiFetchResponse {

  sealed trait Result {

    def id: ResourceRef

    def project: ProjectRef
  }

  object Result {

    sealed trait Error extends Result {
      def reason: String
    }

    final case class AuthorizationFailed(id: ResourceRef, project: ProjectRef) extends Error {
      override def reason: String = "The supplied authentication is not authorized to access this resource."
    }

    final case class NotFound(id: ResourceRef, project: ProjectRef) extends Error {
      override def reason: String = s"The resource '${id.toString}' was not found in project '$project'."
    }

    final case class Success[A](id: ResourceRef, project: ProjectRef, content: JsonLdContent[A]) extends Result

    implicit private val itemErrorEncoder: Encoder.AsObject[Error] = {
      Encoder.AsObject.instance[Error] { r =>
        JsonObject(
          "@type"  -> Json.fromString(r.getClass.getSimpleName),
          "reason" -> Json.fromString(r.reason)
        )
      }
    }

    implicit val itemErrorJsonLdEncoder: JsonLdEncoder[Error] = {
      JsonLdEncoder.computeFromCirce(ContextValue(contexts.error))
    }

    implicit private val api: JsonLdApi = TitaniumJsonLdApi.lenient

    private[model] def itemEncoder(repr: ResourceRepresentation)(implicit base: BaseUri, rcr: RemoteContextResolution) =
      (item: Result) => {
        val common = JsonObject(
          "@id"     -> item.id.asJson,
          "project" -> item.project.asJson
        )

        def valueToJson[A](content: JsonLdContent[A]): IO[Json] = {
          implicit val encoder: JsonLdEncoder[A] = content.encoder
          val value                              = content.resource
          val source                             = content.source
          repr match {
            case SourceJson          => IO.pure(source.asJson)
            case AnnotatedSourceJson => IO.pure(OriginalSource.annotated(value, source).asJson)
            case CompactedJsonLd     => value.toCompactedJsonLd.map { v => v.json }
            case ExpandedJsonLd      => value.toExpandedJsonLd.map { v => v.json }
            case NTriples            => value.toNTriples.map { v => v.value.asJson }
            case NQuads              => value.toNQuads.map { v => v.value.asJson }
            case Dot                 => value.toDot.map { v => v.value.asJson }
          }
        }

        def onError(error: Error): IO[Json] =
          repr match {
            case SourceJson | AnnotatedSourceJson => IO.pure(error.asJson)
            case CompactedJsonLd                  => error.toCompactedJsonLd.map { v => v.json }
            case ExpandedJsonLd                   => error.toExpandedJsonLd.map { v => v.json }
            case NTriples                         => error.toNTriples.map { v => v.value.asJson }
            case NQuads                           => error.toNQuads.map { v => v.value.asJson }
            case Dot                              => error.toDot.map { v => v.value.asJson }
          }

        val result = item match {
          case e: Error               => onError(e).map { e => JsonObject("error" -> e) }
          case Success(_, _, content) => valueToJson(content).map { r => JsonObject("value" -> r) }
        }

        result.map(_.deepMerge(common))
      }

  }

}
