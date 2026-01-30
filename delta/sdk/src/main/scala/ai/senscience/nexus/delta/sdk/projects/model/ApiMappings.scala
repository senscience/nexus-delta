package ai.senscience.nexus.delta.sdk.projects.model

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}
import io.circe.syntax.*
import io.circe.{Decoder, Encoder}

/**
  * The Api mappings to be applied in order to shorten segment ids
  */
final case class ApiMappings(value: Map[String, Iri]) {
  lazy val (prefixMappings, aliases) = value.partition { case (_, iri) => iri.isPrefixMapping }

  /**
    * Append the passed ''that'' [[ApiMappings]] to the current [[ApiMappings]]. If some prefixes are colliding, the
    * ones in the ''that'' [[ApiMappings]] will override the current ones.
    */
  def +(that: ApiMappings): ApiMappings = ApiMappings(value ++ that.value)

  /**
    * Subtract the passed ''that'' [[ApiMappings]] from the current [[ApiMappings]].
    */
  def -(that: ApiMappings): ApiMappings = ApiMappings((value.toSet -- that.value.toSet).toMap)
}

object ApiMappings {

  /**
    * An empty [[ApiMappings]]
    */
  val empty: ApiMappings = ApiMappings(Map.empty[String, Iri])

  /**
    * Construction helper to create [[ApiMappings]] from a collection of segments and their Iris
    */
  def apply(segmentOverrides: (String, Iri)*): ApiMappings = ApiMappings(segmentOverrides.toMap)

  final private case class Mapping(prefix: String, namespace: Iri)

  private given Configuration = Configuration.default.withStrictDecoding

  private given Decoder[Mapping] = deriveConfiguredDecoder[Mapping]
  private given Encoder[Mapping] = deriveConfiguredEncoder[Mapping]

  given Encoder[ApiMappings] =
    Encoder.encodeJson.contramap { case ApiMappings(mappings) =>
      mappings.map { case (prefix, namespace) => Mapping(prefix, namespace) }.asJson
    }

  given Decoder[ApiMappings] =
    Decoder.decodeList[Mapping].map(list => ApiMappings(list.map(m => m.prefix -> m.namespace).toMap))
}
