package ai.senscience.nexus.delta.sdk.resources

import ai.senscience.nexus.delta.kernel.error.Rejection
import ai.senscience.nexus.delta.kernel.utils.UUIDF
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.jsonld.JsonLdAssembly
import ai.senscience.nexus.delta.sdk.jsonld.JsonLdSourceProcessor.JsonLdSourceResolvingParser
import ai.senscience.nexus.delta.sdk.model.IdSegmentRef
import ai.senscience.nexus.delta.sdk.projects.FetchContext
import ai.senscience.nexus.delta.sdk.resolvers.ResolverContextResolution
import ai.senscience.nexus.delta.sdk.resources.Resources.expandIri
import ai.senscience.nexus.delta.sdk.resources.model.{ResourceGenerationResult, ResourceState}
import ai.senscience.nexus.delta.sdk.{DataResource, SchemaResource}
import ai.senscience.nexus.delta.sourcing.model.{ProjectRef, Tags}
import cats.effect.{Clock, IO}
import cats.syntax.all.*
import org.typelevel.otel4s.trace.Tracer

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
    */
  def generate(project: ProjectRef, schema: IdSegmentRef, source: NexusSource)(using
      Caller
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
    */
  def generate(project: ProjectRef, schema: SchemaResource, source: NexusSource)(using
      Caller
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
  def validate(id: IdSegmentRef, project: ProjectRef, schemaOpt: Option[IdSegmentRef])(using
      Caller
  ): IO[ValidationResult]
}

object ResourcesTrial {
  def apply(
      fetchResource: (IdSegmentRef, ProjectRef) => IO[ResourceState],
      validateResource: ValidateResource,
      fetchContext: FetchContext,
      contextResolution: ResolverContextResolution,
      clock: Clock[IO]
  )(using uuidF: UUIDF)(using Tracer[IO]): ResourcesTrial = new ResourcesTrial {

    private val sourceParser = JsonLdSourceResolvingParser(contextResolution, uuidF)

    override def generate(project: ProjectRef, schema: IdSegmentRef, source: NexusSource)(using
        caller: Caller
    ): IO[ResourceGenerationResult] = {
      for {
        projectContext <- fetchContext.onRead(project)
        schemaRef      <- Resources.expandIri(schema, projectContext)
        jsonld         <- sourceParser(project, projectContext, source.value)
        schemaClaim     = SchemaClaim.onCreate(project, schemaRef, caller)
        validation     <- validateResource(jsonld, schemaClaim, projectContext.enforceSchema)
        result         <- toResourceF(project, jsonld, source, validation)
      } yield result
    }.attemptNarrow[Rejection].map { attempt =>
      ResourceGenerationResult(None, attempt)
    }

    override def generate(project: ProjectRef, schema: SchemaResource, source: NexusSource)(using
        Caller
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

    def validate(id: IdSegmentRef, project: ProjectRef, schemaOpt: Option[IdSegmentRef])(using
        caller: Caller
    ): IO[ValidationResult] = {
      for {
        projectContext <- fetchContext.onRead(project)
        schemaRefOpt   <- expandIri(schemaOpt, projectContext)
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
    )(using caller: Caller): IO[DataResource] = {
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
