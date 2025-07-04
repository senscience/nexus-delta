package ai.senscience.nexus.delta.sourcing.partition

import ai.senscience.nexus.delta.rdf.jsonld.context.JsonLdContext.keywords
import ai.senscience.nexus.delta.sourcing.implicits.{jsonbGet, jsonbPut}
import cats.syntax.all.*
import doobie.{Get, Put}
import io.circe.Codec
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveConfiguredCodec
import pureconfig.ConfigReader
import pureconfig.generic.auto.*
import pureconfig.generic.semiauto.deriveReader

/**
  * Partition strategy for the scoped event and state tables in PostgreSQL
  *
  * @see
  *   https://www.postgresql.org/docs/current/ddl-partitioning.html
  */
sealed trait PartitionStrategy

object PartitionStrategy {

  /**
    * Creates a partition per organization and project, fits scenarios where the number of projects is small and the
    * projects can contain a large number of events and resources
    */
  final case object List extends PartitionStrategy

  /**
    * Creates a partition per organization and project, fits scenarios where the number of projects is large * and the
    * projects contain only a small amount of events and resources
    */
  final case class Hash(modulo: Int) extends PartitionStrategy

  implicit val partitionStrategyReader: ConfigReader[PartitionStrategy] = deriveReader[PartitionStrategy]

  implicit val partitionCodec: Codec.AsObject[PartitionStrategy] = {
    implicit val configuration: Configuration = Configuration.default.withDiscriminator(keywords.tpe)
    deriveConfiguredCodec[PartitionStrategy]
  }

  implicit val partitionGet: Get[PartitionStrategy] =
    jsonbGet.temap(v => partitionCodec.decodeJson(v).leftMap(_.message))

  implicit val partitionValue: Put[PartitionStrategy] = jsonbPut.contramap(partitionCodec(_))
}
