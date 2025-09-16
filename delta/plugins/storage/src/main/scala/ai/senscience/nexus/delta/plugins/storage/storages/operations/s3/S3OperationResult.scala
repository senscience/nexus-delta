package ai.senscience.nexus.delta.plugins.storage.storages.operations.s3

sealed trait S3OperationResult extends Product with Serializable

object S3OperationResult {

  case object Success extends S3OperationResult

  case object AlreadyExists extends S3OperationResult

}
