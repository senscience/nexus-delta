package ai.senscience.nexus.delta.plugins.compositeviews.serialization

import ai.senscience.nexus.delta.kernel.utils.UUIDF
import ai.senscience.nexus.delta.plugins.compositeviews.model.{contexts, CompositeViewFields}
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.jsonld.decoder.JsonLdDecoder
import ai.senscience.nexus.delta.rdf.syntax.jsonOpsSyntax
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.jsonld.JsonLdSourceProcessor.JsonLdSourceResolvingDecoder
import ai.senscience.nexus.delta.sdk.projects.model.ProjectContext
import ai.senscience.nexus.delta.sdk.resolvers.ResolverContextResolution
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import cats.effect.IO
import io.circe.Json
import io.circe.syntax.*

import scala.concurrent.duration.FiniteDuration

/**
  * Decoder for [[CompositeViewFields]] which maps some fields to string, before decoding to get around lack of support
  * for @json in json ld library.
  */
//TODO remove when support for @json is added in json-ld library
final class CompositeViewFieldsJsonLdSourceDecoder private (
    decoder: JsonLdSourceResolvingDecoder[CompositeViewFields]
) {
  def apply(ref: ProjectRef, context: ProjectContext, source: Json)(implicit
      caller: Caller
  ): IO[(Iri, CompositeViewFields)] =
    decoder(ref, context, mapJsonToString(source))

  def apply(ref: ProjectRef, context: ProjectContext, iri: Iri, source: Json)(implicit
      caller: Caller
  ): IO[CompositeViewFields] =
    decoder(ref, context, iri, mapJsonToString(source))

  private def mapJsonToString(json: Json): Json = json
    .mapAllKeys("mapping", _.noSpaces.asJson)
    .mapAllKeys("settings", _.noSpaces.asJson)
    .mapAllKeys("context", _.noSpaces.asJson)
}

object CompositeViewFieldsJsonLdSourceDecoder {

  def apply(
      uuidF: UUIDF,
      contextResolution: ResolverContextResolution,
      minIntervalRebuild: FiniteDuration
  ): CompositeViewFieldsJsonLdSourceDecoder = {
    implicit val compositeViewFieldsJsonLdDecoder: JsonLdDecoder[CompositeViewFields] =
      CompositeViewFields.jsonLdDecoder(minIntervalRebuild)
    new CompositeViewFieldsJsonLdSourceDecoder(
      new JsonLdSourceResolvingDecoder[CompositeViewFields](
        contexts.compositeViews,
        contextResolution,
        uuidF
      )
    )
  }

}
