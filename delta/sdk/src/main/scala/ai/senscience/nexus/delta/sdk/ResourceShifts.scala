package ai.senscience.nexus.delta.sdk

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.sdk.jsonld.JsonLdContent
import ai.senscience.nexus.delta.sdk.model.ResourceF
import ai.senscience.nexus.delta.sdk.model.source.OriginalSource
import ai.senscience.nexus.delta.sourcing.implicits.*
import ai.senscience.nexus.delta.sourcing.model.{EntityType, ProjectRef, ResourceRef}
import ai.senscience.nexus.delta.sourcing.state.GraphResource
import ai.senscience.nexus.delta.sourcing.{EntityCheck, Transactors}
import cats.effect.IO
import cats.syntax.all.*
import io.circe.Json

/**
  * Aggregates the different [[ResourceShift]] to perform operations on resources independently of their types
  */
trait ResourceShifts extends GraphResourceEncoder {

  /**
    * Fetch a resource as a [[JsonLdContent]]
    */
  def fetch(reference: ResourceRef, project: ProjectRef): IO[Option[JsonLdContent[?]]]

  /**
    * Return a function to decode a json to a [[GraphResource]] according to its [[EntityType]]
    */
  def decodeGraphResource(entityType: EntityType)(json: Json): IO[GraphResource]

  /**
    * Return a function to decode a json to a [[OriginalSource.Annotated]] according to its [[EntityType]]
    */
  def decodeAnnotatedSource(entityType: EntityType)(json: Json): IO[OriginalSource.Annotated]

  def encodeResource[A](entityType: EntityType)(project: ProjectRef, resource: ResourceF[A]): IO[GraphResource]
}

object ResourceShifts {

  private val logger = Logger[ResourceShifts]

  private case class NoShiftAvailable(entityType: EntityType)
      extends Exception(s"No shift is available for entity type $entityType")

  def apply(shifts: Set[ResourceShift[?, ?]], xas: Transactors)(using RemoteContextResolution): ResourceShifts =
    new ResourceShifts {
      private val shiftsMap = shifts.map { encoder => encoder.entityType -> encoder }.toMap

      private def findShift(entityType: EntityType): IO[ResourceShift[?, ?]] = IO
        .fromOption(shiftsMap.get(entityType))(NoShiftAvailable(entityType))

      override def fetch(reference: ResourceRef, project: ProjectRef): IO[Option[JsonLdContent[?]]] =
        for {
          entityType <- EntityCheck.findType(reference.iri, project, xas)
          shift      <- entityType.traverse(findShift)
          resource   <- shift.flatTraverse(_.fetch(reference, project))
        } yield resource

      override def decodeGraphResource(entityType: EntityType)(json: Json): IO[GraphResource] =
        findShift(entityType)
          .flatMap(_.toGraphResource(json))
          .onError { case err => logDecodingError(err)(entityType, GraphResource.getClass) }

      override def decodeAnnotatedSource(entityType: EntityType)(json: Json): IO[OriginalSource.Annotated] =
        findShift(entityType)
          .flatMap(_.toAnnotatedSource(json))
          .onError { case err => logDecodingError(err)(entityType, OriginalSource.Annotated.getClass) }

      private def logDecodingError(err: Throwable)(entityType: EntityType, clazz: Class[?]) =
        logger.error(err)(s"Entity of type '$entityType' could not be decoded as ${clazz.getSimpleName}")

      override def encodeResource[A](
          entityType: EntityType
      )(project: ProjectRef, resource: ResourceF[A]): IO[GraphResource] =
        findShift(entityType).flatMap(_.asInstanceOf[ResourceShift[?, A]].toGraphResource(project, resource))
    }
}
