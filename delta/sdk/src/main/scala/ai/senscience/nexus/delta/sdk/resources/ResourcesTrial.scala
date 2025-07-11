package ai.senscience.nexus.delta.sdk.resources

import ai.senscience.nexus.delta.kernel.error.Rejection
import ai.senscience.nexus.delta.kernel.utils.UUIDF
import ai.senscience.nexus.delta.sdk.DataResource
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.jsonld.JsonLdAssembly
import ai.senscience.nexus.delta.sdk.jsonld.JsonLdSourceProcessor.JsonLdSourceResolvingParser
import ai.senscience.nexus.delta.sdk.model.{IdSegment, IdSegmentRef, ResourceF}
import ai.senscience.nexus.delta.sdk.projects.FetchContext
import ai.senscience.nexus.delta.sdk.resolvers.ResolverContextResolution
import ai.senscience.nexus.delta.sdk.resources.Resources.expandResourceRef
import ai.senscience.nexus.delta.sdk.resources.model.{ResourceGenerationResult, ResourceState}
import ai.senscience.nexus.delta.sdk.schemas.model.Schema
import ai.senscience.nexus.delta.sourcing.model.{ProjectRef, Tags}
import cats.effect.{Clock, IO}
import cats.implicits.*

/**
  * Operations allowing to perform read-only operations on resources
  */
trait ResourcesTrial {

  /**
    * Generates the resource and validate it against the provided schema reference
    * @param project
    *   the enclosing project
    * @param schema
    *   the schema reference to validate on
    * @param source
    *   the original json payload
    * @param caller
    *   the user performing the action
    */
  def generate(project: ProjectRef, schema: IdSegment, source: NexusSource)(implicit
      caller: Caller
  ): IO[ResourceGenerationResult]

  /**
    * Generates the resource and validate it against the provided schema
    *
    * @param project
    *   the enclosing project
    * @param schema
    *   the schema to validate on
    * @param source
    *   the original json payload
    * @param caller
    *   the user performing the action
    */
  def generate(project: ProjectRef, schema: ResourceF[Schema], source: NexusSource)(implicit
      caller: Caller
  ): IO[ResourceGenerationResult]

  /**
    * Validates an existing resource.
    *
    * @param id
    *   the identifier that will be expanded to the Iri of the resource
    * @param project
    *   the project reference where the resource belongs
    * @param schemaOpt
    *   the optional identifier that will be expanded to the schema reference to validate the resource. A None value
    *   uses the currently available resource schema reference.
    */
  def validate(id: IdSegmentRef, project: ProjectRef, schemaOpt: Option[IdSegment])(implicit
      caller: Caller
  ): IO[ValidationResult]
}

object ResourcesTrial {
  def apply(
      fetchResource: (IdSegmentRef, ProjectRef) => IO[ResourceState],
      validateResource: ValidateResource,
      fetchContext: FetchContext,
      contextResolution: ResolverContextResolution,
      clock: Clock[IO]
  )(implicit uuidF: UUIDF): ResourcesTrial = new ResourcesTrial {

    private val sourceParser = JsonLdSourceResolvingParser(contextResolution, uuidF)

    override def generate(project: ProjectRef, schema: IdSegment, source: NexusSource)(implicit
        caller: Caller
    ): IO[ResourceGenerationResult] = {
      for {
        projectContext <- fetchContext.onRead(project)
        schemaRef      <- IO.fromEither(Resources.expandResourceRef(schema, projectContext))
        jsonld         <- sourceParser(project, projectContext, source.value)
        schemaClaim     = SchemaClaim.onCreate(project, schemaRef, caller)
        validation     <- validateResource(jsonld, schemaClaim, projectContext.enforceSchema)
        result         <- toResourceF(project, jsonld, source, validation)
      } yield result
    }.attemptNarrow[Rejection].map { attempt =>
      ResourceGenerationResult(None, attempt)
    }

    override def generate(project: ProjectRef, schema: ResourceF[Schema], source: NexusSource)(implicit
        caller: Caller
    ): IO[ResourceGenerationResult] = {
      for {
        projectContext <- fetchContext.onRead(project)
        jsonld         <- sourceParser(project, projectContext, source.value)
        validation     <- validateResource(jsonld, schema)
        result         <- toResourceF(project, jsonld, source, validation)
      } yield result
    }.attemptNarrow[Rejection].map { attempt =>
      ResourceGenerationResult(Some(schema), attempt)
    }

    def validate(id: IdSegmentRef, project: ProjectRef, schemaOpt: Option[IdSegment])(implicit
        caller: Caller
    ): IO[ValidationResult] = {
      for {
        projectContext <- fetchContext.onRead(project)
        schemaRefOpt   <- IO.fromEither(expandResourceRef(schemaOpt, projectContext))
        resource       <- fetchResource(id, project)
        jsonld         <- resource.toAssembly
        schemaClaim     = SchemaClaim.onUpdate(project, schemaRefOpt, resource.schema, caller)
        report         <- validateResource(jsonld, schemaClaim, projectContext.enforceSchema)
      } yield report
    }

    private def toResourceF(
        project: ProjectRef,
        jsonld: JsonLdAssembly,
        source: NexusSource,
        validation: ValidationResult
    )(implicit caller: Caller): IO[DataResource] = {
      clock.realTimeInstant.map { now =>
        ResourceState(
          id = jsonld.id,
          project = project,
          schemaProject = validation.project,
          source = source.value,
          compacted = jsonld.compacted,
          expanded = jsonld.expanded,
          remoteContexts = jsonld.remoteContexts,
          rev = 1,
          deprecated = false,
          schema = validation.schema,
          types = jsonld.types,
          tags = Tags.empty,
          createdAt = now,
          createdBy = caller.subject,
          updatedAt = now,
          updatedBy = caller.subject
        ).toResource
      }

    }

  }

}
