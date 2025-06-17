package ai.senscience.nexus.delta.sdk.schemas.job

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.rdf.Vocabulary.schemas
import ai.senscience.nexus.delta.sdk.resources.ValidateResource
import ai.senscience.nexus.delta.sdk.resources.model.{ResourceRejection, ResourceState}
import ai.senscience.nexus.delta.sdk.schemas.FetchSchema
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.model.ResourceRef.Latest
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.stream.{ElemStream, FailureReason, SuccessElemStream}
import cats.effect.IO
import cats.syntax.all.*
import fs2.Stream

/**
  * Streams the latest version of resources from a project and revalidate them with the latest version of the schema
  * they are currently validated with.
  *   - Only data resources are evaluated
  *   - Deprecated resources are skipped
  *   - Resources not validated with a schema are skipped too
  */
trait SchemaValidationStream {

  def apply(project: ProjectRef, offset: Offset): ElemStream[Unit]
}

object SchemaValidationStream {

  private val logger = Logger[SchemaValidationStream]

  def apply(
      resourceStream: (ProjectRef, Offset) => SuccessElemStream[ResourceState],
      fetchSchema: FetchSchema,
      validateResource: ValidateResource
  ): SchemaValidationStream = new SchemaValidationStream {

    private def validateSingle(resource: ResourceState) =
      for {
        jsonld <- resource.toAssembly
        schema <- fetchSchema(Latest(resource.schema.iri), resource.schemaProject)
        _      <- validateResource(jsonld, schema).adaptErr { case r: ResourceRejection =>
                    FailureReason("ValidateSchema", r)
                  }
      } yield (Some(()))

    override def apply(project: ProjectRef, offset: Offset): ElemStream[Unit] =
      Stream.eval(logger.info(s"Starting validation of resources for project '$project'")) >>
        resourceStream(project, offset)
          .evalMap {
            _.evalMapFilter {
              case r if r.deprecated                      => IO.none
              case r if r.schema.iri == schemas.resources => IO.none
              case r                                      => validateSingle(r)
            }
          }
          .onFinalize {
            logger.info(s"Validation of resources for project '$project' has been completed.")
          }
  }
}
