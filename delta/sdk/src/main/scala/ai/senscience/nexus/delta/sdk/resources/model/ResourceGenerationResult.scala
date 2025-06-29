package ai.senscience.nexus.delta.sdk.resources.model

import ai.senscience.nexus.delta.kernel.error.Rejection
import ai.senscience.nexus.delta.kernel.utils.ClassUtils
import ai.senscience.nexus.delta.rdf.jsonld.api.{JsonLdApi, TitaniumJsonLdApi}
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.sdk.jsonld.JsonLdRejection
import ai.senscience.nexus.delta.sdk.model.{BaseUri, ResourceF}
import ai.senscience.nexus.delta.sdk.resources.model.ResourceGenerationResult.*
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sdk.{DataResource, SchemaResource}
import cats.effect.IO
import io.circe.Json
import io.circe.syntax.KeyOps

/**
  * Result of the generation of a resource
  * @param schema
  *   the schema if it has been generated
  * @param attempt
  *   the result of the generation attempt
  */
final case class ResourceGenerationResult(
    schema: Option[SchemaResource],
    attempt: Either[Rejection, DataResource]
) {

  def asJson(implicit base: BaseUri, rcr: RemoteContextResolution): IO[Json] = {
    for {
      schema          <- schema.fold(emptySchema)(toJsonField("schema", _))
      resourceOrError <- attempt.fold(
                           errorField,
                           toJsonField("result", _)
                         )
    } yield schema deepMerge resourceOrError
  }

  private def errorField(rejection: Rejection)(implicit rcr: RemoteContextResolution) = rejection match {
    case rejection: ResourceRejection => toJsonField("error": String, rejection)
    case rejection: JsonLdRejection   => toJsonField("error": String, rejection)
    case other: Rejection             => IO.pure(Json.obj("@type" := ClassUtils.simpleName(other), "reason" := other.reason))
  }

  private def toJsonField[A](fieldName: String, value: A)(implicit
      encoder: JsonLdEncoder[A],
      rcr: RemoteContextResolution
  ) =
    value.toCompactedJsonLd.map { v => v.json }.map { s => Json.obj(fieldName -> s) }

  private def toJsonField[A](fieldName: String, value: ResourceF[A])(implicit
      encoder: JsonLdEncoder[A],
      base: BaseUri,
      rcr: RemoteContextResolution
  ) =
    value.toCompactedJsonLd.map { v => v.json }.map { s => Json.obj(fieldName -> s) }
}

object ResourceGenerationResult {
  implicit private[model] val api: JsonLdApi = TitaniumJsonLdApi.lenient

  val emptySchema: IO[Json] = IO.pure(Json.obj())
}
