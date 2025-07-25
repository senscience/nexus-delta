package ai.senscience.nexus.delta.plugins.storage.storages.model

import ai.senscience.nexus.delta.plugins.storage.storages.nxvStorage
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import io.circe.{Decoder, Encoder, Json}

/**
  * Enumeration of Storage types.
  */
sealed trait StorageType extends Product with Serializable {

  /**
    * @return
    *   the type id
    */
  def iri: Iri

  /**
    * The JSON-LD types for the given storage type
    */
  def types: Set[Iri] = Set(nxvStorage, iri)
}

object StorageType {

  def apply(iri: Iri): Either[String, StorageType] =
    if (iri == DiskStorage.iri) Right(DiskStorage)
    else if (iri == S3Storage.iri) Right(S3Storage)
    else if (iri == RemoteDiskStorage.iri) Right(RemoteDiskStorage)
    else Left(s"iri '$iri' does not match a StorageType")

  /**
    * A local disk storage type.
    */
  final case object DiskStorage extends StorageType {
    override val toString: String = "DiskStorage"
    override val iri: Iri         = nxv + toString
  }

  /**
    * An S3 compatible storage type.
    */
  final case object S3Storage extends StorageType {
    override val toString: String = "S3Storage"
    override val iri: Iri         = nxv + toString
  }

  /**
    * A remote disk storage type.
    */
  final case object RemoteDiskStorage extends StorageType {
    override val toString: String = "RemoteDiskStorage"
    override val iri: Iri         = nxv + toString
  }

  implicit final val storageTypeEncoder: Encoder[StorageType] = Encoder.instance {
    case DiskStorage       => Json.fromString("DiskStorage")
    case S3Storage         => Json.fromString("S3Storage")
    case RemoteDiskStorage => Json.fromString("RemoteDiskStorage")
  }

  implicit final val storageTypeDecoder: Decoder[StorageType] = Decoder.decodeString.emap {
    case "DiskStorage"       => Right(DiskStorage)
    case "S3Storage"         => Right(S3Storage)
    case "RemoteDiskStorage" => Right(RemoteDiskStorage)
  }
}
