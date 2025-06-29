package ai.senscience.nexus.delta.sourcing.model

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.contexts
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.sourcing.implicits.*
import ai.senscience.nexus.delta.sourcing.model.FailedElemLogRow.FailedElemData
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.stream.{FailureReason, ProjectionMetadata}
import doobie.*
import doobie.postgres.implicits.*
import io.circe.generic.semiauto.deriveEncoder
import io.circe.{Encoder, Json}

import java.time.Instant

/**
  * The row of the failed_elem_log table
  */
final case class FailedElemLogRow(
    ordering: Offset,
    projectionMetadata: ProjectionMetadata,
    failedElemData: FailedElemData,
    instant: Instant
)

object FailedElemLogRow {
  private type Row =
    (
        Offset,
        String,
        String,
        Option[ProjectRef],
        Option[Iri],
        EntityType,
        Offset,
        Iri,
        Option[ProjectRef],
        Int,
        String,
        Option[String],
        Option[String],
        Instant,
        Option[Json]
    )

  /**
    * Helper case class to structure FailedElemLogRow
    */
  final case class FailedElemData(
      id: Iri,
      project: Option[ProjectRef],
      entityType: EntityType,
      offset: Offset,
      rev: Int,
      reason: FailureReason
  )

  val context: ContextValue = ContextValue(contexts.error)

  implicit val failedElemDataEncoder: Encoder.AsObject[FailedElemData] =
    deriveEncoder[FailedElemData].mapJsonObject(_.remove("entityType"))

  implicit val failedElemDataJsonLdEncoder: JsonLdEncoder[FailedElemData] =
    JsonLdEncoder.computeFromCirce(ContextValue(contexts.error))

  implicit val failedElemLogRow: Read[FailedElemLogRow] = {
    Read[Row].map {
      case (
            ordering,
            name,
            module,
            project,
            resourceId,
            entityType,
            elemOffset,
            elemId,
            elemProject,
            revision,
            errorType,
            message,
            stackTrace,
            instant,
            details
          ) =>
        val reason = details
          .map { d =>
            FailureReason(errorType, d)
          }
          .getOrElse(FailureReason(errorType, message.getOrElse(""), stackTrace))

        FailedElemLogRow(
          ordering,
          ProjectionMetadata(module, name, project, resourceId),
          FailedElemData(elemId, elemProject, entityType, elemOffset, revision, reason),
          instant
        )
    }
  }
}
