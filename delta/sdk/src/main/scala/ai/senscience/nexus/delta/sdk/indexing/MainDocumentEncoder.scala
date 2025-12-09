package ai.senscience.nexus.delta.sdk.indexing

import ai.senscience.nexus.delta.sdk.indexing.MainDocument
import ai.senscience.nexus.delta.sdk.model.ResourceF
import ai.senscience.nexus.delta.sourcing.model.EntityType
import ai.senscience.nexus.delta.sourcing.state.State.ScopedState
import cats.effect.IO
import io.circe.{Decoder, Json}

abstract class MainDocumentEncoder[State <: ScopedState, A] {

  def entityType: EntityType

  def databaseDecoder: Decoder[State]

  protected def toResourceF(state: State): ResourceF[A]

  def fromResource(resource: ResourceF[A]): MainDocument

  def fromJson(json: Json): IO[MainDocument] =
    IO.fromEither(databaseDecoder.decodeJson(json))
      .map { toResourceF.andThen(fromResource) }
}

object MainDocumentEncoder {

  private case class NoEncoderAvailable(entityType: EntityType)
      extends Exception(s"No main document encoder is available for entity type $entityType")

  trait Aggregate {
    def fromJson(entityType: EntityType)(json: Json): IO[MainDocument]

    def fromResource[A](entityType: EntityType)(resource: ResourceF[A]): IO[MainDocument]
  }

  object Aggregate {
    def apply(underlying: Set[MainDocumentEncoder[?, ?]]): Aggregate = new Aggregate {
      private val encoderMap = underlying.map { encoder => encoder.entityType -> encoder }.toMap

      private def findEncoder(entityType: EntityType): IO[MainDocumentEncoder[?, ?]] =
        IO.fromOption(encoderMap.get(entityType))(NoEncoderAvailable(entityType))

      def fromJson(entityType: EntityType)(json: Json): IO[MainDocument] =
        findEncoder(entityType).flatMap {
          _.fromJson(json)
        }

      def fromResource[A](entityType: EntityType)(resource: ResourceF[A]): IO[MainDocument] =
        findEncoder(entityType).map {
          _.asInstanceOf[MainDocumentEncoder[?, A]].fromResource(resource)
        }
    }
  }

}
