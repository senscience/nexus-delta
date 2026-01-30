package ai.senscience.nexus.delta.sdk.permissions.model

import ai.senscience.nexus.delta.rdf.Vocabulary.contexts
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import io.circe.generic.semiauto.*
import io.circe.{Decoder, Encoder}

/**
  * A wrapper for a collection of permissions
  */
final case class PermissionSet(permissions: Set[Permission])

object PermissionSet {

  given Encoder.AsObject[PermissionSet] = deriveEncoder
  given Decoder[PermissionSet]          = deriveDecoder

  given JsonLdEncoder[PermissionSet] = JsonLdEncoder.computeFromCirce(ContextValue(contexts.permissions))

}
