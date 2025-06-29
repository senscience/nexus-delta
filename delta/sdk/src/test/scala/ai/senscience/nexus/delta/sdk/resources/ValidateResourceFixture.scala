package ai.senscience.nexus.delta.sdk.resources

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.shacl.ValidationReport
import ai.senscience.nexus.delta.sdk.SchemaResource
import ai.senscience.nexus.delta.sdk.jsonld.JsonLdAssembly
import ai.senscience.nexus.delta.sdk.model.ResourceF
import ai.senscience.nexus.delta.sdk.resolvers.model.ResourceResolutionReport
import ai.senscience.nexus.delta.sdk.resources.SchemaClaim.DefinedSchemaClaim
import ai.senscience.nexus.delta.sdk.resources.ValidationResult.*
import ai.senscience.nexus.delta.sdk.resources.model.ResourceRejection
import ai.senscience.nexus.delta.sdk.resources.model.ResourceRejection.InvalidSchemaRejection
import ai.senscience.nexus.delta.sdk.schemas.model.Schema
import ai.senscience.nexus.delta.sourcing.model.{ProjectRef, ResourceRef}
import cats.effect.IO
import io.circe.Json
import io.circe.syntax.KeyOps

trait ValidateResourceFixture {

  val defaultReport: ValidationReport = ValidationReport.unsafe(conforms = true, 5, Json.obj("conforms" := "true"))
  val defaultSchemaRevision           = 1

  def alwaysValidate: ValidateResource = new ValidateResource {
    override def apply(jsonld: JsonLdAssembly, schema: SchemaClaim, enforceSchema: Boolean): IO[ValidationResult] =
      IO.pure(
        schema match {
          case defined: DefinedSchemaClaim =>
            Validated(
              schema.project,
              ResourceRef.Revision(defined.schemaRef.iri, defaultSchemaRevision),
              defaultReport
            )
          case other                       => NoValidation(other.project)
        }
      )

    override def apply(
        jsonld: JsonLdAssembly,
        schema: ResourceF[Schema]
    ): IO[ValidationResult] =
      IO.pure(
        Validated(
          schema.value.project,
          ResourceRef.Revision(schema.id, defaultSchemaRevision),
          defaultReport
        )
      )
  }

  def alwaysFail(expected: ResourceRejection): ValidateResource = new ValidateResource {
    override def apply(jsonld: JsonLdAssembly, schema: SchemaClaim, enforceSchema: Boolean): IO[ValidationResult] =
      IO.raiseError(expected)

    override def apply(
        jsonld: JsonLdAssembly,
        schema: ResourceF[Schema]
    ): IO[ValidationResult] = IO.raiseError(expected)
  }

  def validateFor(validSchemas: Set[(ProjectRef, Iri)]): ValidateResource =
    new ValidateResource {
      override def apply(jsonld: JsonLdAssembly, schema: SchemaClaim, enforceSchema: Boolean): IO[ValidationResult] = {
        val project = schema.project
        schema match {
          case defined: DefinedSchemaClaim if validSchemas.contains((project, defined.schemaRef.iri)) =>
            val schemaRevision = ResourceRef.Revision(defined.schemaRef.iri, defaultSchemaRevision)
            IO.pure(Validated(project, schemaRevision, defaultReport))
          case defined: DefinedSchemaClaim                                                            =>
            IO.raiseError(InvalidSchemaRejection(defined.schemaRef, project, ResourceResolutionReport()))
          case other                                                                                  => IO.pure(NoValidation(other.project))
        }
      }

      override def apply(
          jsonld: JsonLdAssembly,
          schema: ResourceF[Schema]
      ): IO[ValidationResult] =
        IO.pure(
          Validated(
            schema.value.project,
            ResourceRef.Revision(schema.id, schema.rev),
            defaultReport
          )
        )
    }

  def validateForResources(validResources: Set[Iri]): ValidateResource = new ValidateResource {

    override def apply(jsonld: JsonLdAssembly, schema: SchemaClaim, enforceSchema: Boolean): IO[ValidationResult] = ???

    override def apply(jsonld: JsonLdAssembly, schema: SchemaResource): IO[ValidationResult] =
      if (validResources.contains(jsonld.id)) {
        IO.pure(
          Validated(
            schema.value.project,
            ResourceRef.Revision(schema.id, schema.rev),
            defaultReport
          )
        )
      } else
        IO.raiseError(
          InvalidSchemaRejection(
            ResourceRef.Revision(schema.id, schema.rev),
            schema.value.project,
            ResourceResolutionReport()
          )
        )
  }

}
