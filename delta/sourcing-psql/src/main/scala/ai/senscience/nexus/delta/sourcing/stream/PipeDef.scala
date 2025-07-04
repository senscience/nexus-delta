package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.rdf.jsonld.ExpandedJsonLd
import ai.senscience.nexus.delta.rdf.jsonld.decoder.JsonLdDecoder
import ai.senscience.nexus.delta.sourcing.stream.Operation.Pipe
import ai.senscience.nexus.delta.sourcing.stream.ProjectionErr.CouldNotDecodePipeConfigErr
import cats.implicits.*
import shapeless.Typeable

/**
  * Contract for definition for pipes. PipeDefs are used to construct [[Pipe]] instances to be used when materializing
  * runnable projections. They are indexed by their label in a registry such they can be looked up given a [[PipeRef]].
  * Provided with a configuration, pipe definitions can produce [[Pipe]] instances.
  */
trait PipeDef {

  /**
    * The type of the [[Pipe]] that this definition produces.
    */
  type PipeType <: Pipe

  /**
    * The required configuration type for producing a [[Pipe]] of this type.
    */
  type Config

  /**
    * @return
    *   the Typeable instance for the required [[Pipe]] configuration
    */
  def configType: Typeable[Config]

  /**
    * @return
    *   a json-ld decoder for the [[Pipe]] configuration
    */
  def configDecoder: JsonLdDecoder[Config]

  /**
    * @return
    *   the unique reference for a pipe of this type
    */
  def ref: PipeRef

  /**
    * Produces a [[Pipe]] instance given an expected configuration.
    *
    * @param config
    *   the configuration for the [[PipeDef]]
    */
  def withConfig(config: Config): PipeType

  /**
    * Attempts to construct a corresponding [[Pipe]] instance by decoding the required configuration from a json-ld
    * configuration.
    *
    * @param jsonLd
    *   the source configuration in the json-ld format
    */
  def withJsonLdConfig(jsonLd: ExpandedJsonLd): Either[CouldNotDecodePipeConfigErr, PipeType] =
    configDecoder(jsonLd)
      .map(c => withConfig(c))
      .leftMap(e => CouldNotDecodePipeConfigErr(jsonLd, configType.describe, ref, e.reason))

}
