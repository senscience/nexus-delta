package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.rdf.Vocabulary.contexts
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue
import ai.senscience.nexus.delta.rdf.jsonld.context.JsonLdContext.keywords
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import io.circe.Codec
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveConfiguredCodec

import java.time.Instant

/**
  * Describes the remaining elements to stream
  * @param count
  *   the number of remaining elements
  * @param maxInstant
  *   the instant of the last element
  */
final case class RemainingElems(count: Long, maxInstant: Instant)

object RemainingElems {

  given Codec[RemainingElems] = {
    given Configuration = Configuration.default.withDiscriminator(keywords.tpe)
    deriveConfiguredCodec[RemainingElems]
  }

  given JsonLdEncoder[RemainingElems] = JsonLdEncoder.computeFromCirce(ContextValue(contexts.offset))

}
