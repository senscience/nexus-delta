package ai.senscience.nexus.delta.sdk.marshalling

/**
  * Enumeration of allowed Json-LD output formats on the service
  */
enum JsonLdFormat {

  /**
    * Expanded JSON-LD output format as defined in https://www.w3.org/TR/json-ld-api/#expansion-algorithms
    */
  case Expanded

  /**
    * Compacted JSON-LD output format as defined in https://www.w3.org/TR/json-ld-api/#compaction-algorithms
    */
  case Compacted
}
