package ai.senscience.nexus.delta.plugins.storage.storages.model

import ai.senscience.nexus.delta.plugins.storage.storages.nxvStorage
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import io.circe.{Decoder, Encoder, Json}

/**
  * Enumeration of Storage types.
  */
enum StorageType(suffix: String) {
  override val toString: String = suffix
  def iri: Iri                  = nxv + toString
  def types: Set[Iri]           = Set(nxvStorage, iri)

  /**
    * A local disk storage type.
    */
  case DiskStorage extends StorageType("DiskStorage")

  /**
    * An S3 compatible storage type.
    */
  case S3Storage extends StorageType("S3Storage")
}

object StorageType {

  def apply(iri: Iri): Either[String, StorageType] =
    if iri == DiskStorage.iri then Right(DiskStorage)
    else if iri == S3Storage.iri then Right(S3Storage)
    else Left(s"iri '$iri' does not match a StorageType")

  given Encoder[StorageType] = Encoder.instance {
    case DiskStorage => Json.fromString("DiskStorage")
    case S3Storage   => Json.fromString("S3Storage")
  }

  given Decoder[StorageType] = Decoder.decodeString.emap {
    case "DiskStorage" => Right(DiskStorage)
    case "S3Storage"   => Right(S3Storage)
  }
}
