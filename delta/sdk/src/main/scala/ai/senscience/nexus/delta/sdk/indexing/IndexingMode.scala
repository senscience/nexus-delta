package ai.senscience.nexus.delta.sdk.indexing

/**
  * Enumeration of all possible indexing modes
  */
enum IndexingMode {

  /**
    * Asynchronously indexing resources
    */
  case Async

  /**
    * Synchronously indexing resources
    */
  case Sync
}
