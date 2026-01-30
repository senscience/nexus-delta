package ai.senscience.nexus.delta.sdk.views

import ai.senscience.nexus.delta.rdf.Vocabulary.{contexts, nxv}
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue
import ai.senscience.nexus.delta.rdf.jsonld.decoder.configuration.semiauto.deriveConfigJsonLdDecoder
import ai.senscience.nexus.delta.rdf.jsonld.decoder.{Configuration, JsonLdDecoder}
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.rdf.jsonld.{ExpandedJsonLd, ExpandedJsonLdCursor}
import ai.senscience.nexus.delta.sourcing.model.Label
import ai.senscience.nexus.delta.sourcing.stream.PipeRef
import cats.implicits.*
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}

/**
  * Definition of a pipe to include in a view
  * @param name
  *   the identifier of the pipe to apply
  * @param description
  *   a description of what is expected from the pipe
  * @param config
  *   the config to provide to the pipe
  */
final case class PipeStep(name: Label, description: Option[String], config: Option[ExpandedJsonLd]) {

  def description(value: String): PipeStep = copy(description = Some(value))

}

object PipeStep {

  def apply(value: (PipeRef, ExpandedJsonLd)): PipeStep =
    apply(value._1.label, value._2)

  def apply(name: Label, cfg: ExpandedJsonLd): PipeStep =
    PipeStep(name, None, Some(cfg))

  /**
    * Create a pipe def without config
    * @param ref
    *   the reference of the pipe
    */
  def noConfig(ref: PipeRef): PipeStep = PipeStep(ref.label, None, None)

  /**
    * Create a pipe with the provided config
    * @param name
    *   the identifier of the pipe
    * @param config
    *   the config to apply
    */
  def withConfig(name: Label, config: ExpandedJsonLd): PipeStep = PipeStep(name, None, Some(config))

  given Encoder.AsObject[PipeStep] = {
    given Encoder[ExpandedJsonLd] = Encoder.instance(_.json)
    deriveEncoder[PipeStep]
  }

  given Decoder[PipeStep] = {
    given Decoder[ExpandedJsonLd] =
      Decoder.decodeJson.emap(ExpandedJsonLd.expanded(_).leftMap(_.getMessage))
    deriveDecoder[PipeStep].map {
      case p if p.config.isDefined =>
        val config = p.config.map { e => ExpandedJsonLd.unsafe(nxv + p.name.value, e.obj) }
        p.copy(config = config)
      case p                       => p
    }
  }

  given JsonLdEncoder[PipeStep] =
    JsonLdEncoder.computeFromCirce(ContextValue(contexts.pipeline))

  given pipeStepJsonLdDecoder: Configuration => JsonLdDecoder[PipeStep] = {
    given JsonLdDecoder[ExpandedJsonLd] = (cursor: ExpandedJsonLdCursor) => cursor.focus
    deriveConfigJsonLdDecoder[PipeStep].map {
      case p if p.config.isDefined =>
        val config = p.config.map { e => ExpandedJsonLd.unsafe(nxv + p.name.value, e.obj) }
        p.copy(config = config)
      case p                       => p
    }
  }
}
