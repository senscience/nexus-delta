package ai.senscience.nexus.delta.sdk

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.sdk.jsonld.JsonLdContent
import ai.senscience.nexus.delta.sourcing.implicits.*
import ai.senscience.nexus.delta.sourcing.model.{EntityType, ProjectRef, ResourceRef}
import ai.senscience.nexus.delta.sourcing.state.GraphResource
import ai.senscience.nexus.delta.sourcing.{EntityCheck, Transactors}
import cats.data.NonEmptyList
import cats.effect.IO
import cats.syntax.all.*
import io.circe.Json

/**
  * Aggregates the different [[ResourceShift]] to perform operations on resources independently of their types
  */
trait ResourceShifts {

  def entityTypes: Option[NonEmptyList[EntityType]]

  /**
    * Fetch a resource as a [[JsonLdContent]]
    */
  def fetch(reference: ResourceRef, project: ProjectRef): IO[Option[JsonLdContent[?]]]

  /**
    * Return a function to decode a json to a [[GraphResource]] according to its [[EntityType]]
    */
  def decodeGraphResource: (EntityType, Json) => IO[GraphResource]

}

object ResourceShifts {

  private val logger = Logger[ResourceShifts]

  private case class NoShiftAvailable(entityType: EntityType)
      extends Exception(s"No shift is available for entity type $entityType")

  def apply(shifts: Set[ResourceShift[?, ?]], xas: Transactors)(implicit
      cr: RemoteContextResolution
  ): ResourceShifts = new ResourceShifts {
    private val shiftsMap = shifts.map { encoder => encoder.entityType -> encoder }.toMap

    override def entityTypes: Option[NonEmptyList[EntityType]] = NonEmptyList.fromList(shiftsMap.keys.toList)

    private def findShift(entityType: EntityType): IO[ResourceShift[?, ?]] = IO
      .fromOption(shiftsMap.get(entityType))(
        NoShiftAvailable(entityType)
      )

    override def fetch(reference: ResourceRef, project: ProjectRef): IO[Option[JsonLdContent[?]]] =
      for {
        entityType <- EntityCheck.findType(reference.iri, project, xas)
        shift      <- entityType.traverse(findShift)
        resource   <- shift.flatTraverse(_.fetch(reference, project))
      } yield resource

    override def decodeGraphResource: (EntityType, Json) => IO[GraphResource] = {
      (entityType: EntityType, json: Json) =>
        {
          for {
            shift  <- findShift(entityType)
            result <- shift.toGraphResource(json)
          } yield result
        }.onError { case err =>
          logger.error(err)(s"Entity of type '$entityType' could not be decoded as a graph resource")
        }
    }
  }
}
