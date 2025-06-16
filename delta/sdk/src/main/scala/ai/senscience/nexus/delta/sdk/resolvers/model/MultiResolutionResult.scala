package ai.senscience.nexus.delta.sdk.resolvers.model

import ai.senscience.nexus.delta.sdk.jsonld.JsonLdContent

/**
  * Result of a MultiResolution
  */
final case class MultiResolutionResult[+R](report: R, value: JsonLdContent[?])
