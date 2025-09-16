package ai.senscience.nexus.delta.sdk.indexing

/**
  * Enumeration of all possible indexing modes
  */
sealed trait IndexingMode extends Product with Serializable

object IndexingMode {

  /**
    * Asynchronously indexing resources
    */
  case object Async extends IndexingMode

  /**
    * Synchronously indexing resources
    */
  case object Sync extends IndexingMode
}
