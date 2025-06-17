package ai.senscience.nexus.delta.sdk.resources

import ai.senscience.nexus.delta.kernel.syntax.*
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary
import ai.senscience.nexus.delta.rdf.Vocabulary.contexts
import ai.senscience.nexus.delta.rdf.shacl.{ValidateShacl, ValidationReport}
import ai.senscience.nexus.delta.sdk.SchemaResource
import ai.senscience.nexus.delta.sdk.jsonld.JsonLdAssembly
import ai.senscience.nexus.delta.sdk.model.ResourceF
import ai.senscience.nexus.delta.sdk.resources.Resources.kamonComponent
import ai.senscience.nexus.delta.sdk.resources.ValidationResult.{NoValidation, Validated}
import ai.senscience.nexus.delta.sdk.resources.model.ResourceRejection.{InvalidResource, NoTargetedNode, ReservedResourceId, ReservedResourceTypes, ResourceShaclEngineRejection}
import ai.senscience.nexus.delta.sdk.schemas.model.Schema
import ai.senscience.nexus.delta.sourcing.model.ResourceRef
import cats.effect.IO

/**
  * Allows to validate the resource:
  *   - Validate it against the provided schema
  *   - Checking if the provided resource id is not reserved
  *   - Checking if the provided resource types are not reserved
  */
trait ValidateResource {

  /**
    * Validate against a schema reference
    *
    * @param jsonld
    *   the generated resource
    * @param schema
    *   the schema claim
    * @param enforceSchema
    *   whether to ban unconstrained resources
    */
  def apply(jsonld: JsonLdAssembly, schema: SchemaClaim, enforceSchema: Boolean): IO[ValidationResult]

  /**
    * Validate against a schema
    *
    * @param jsonld
    *   the generated resource
    * @param schema
    *   the schema to validate against
    */
  def apply(jsonld: JsonLdAssembly, schema: SchemaResource): IO[ValidationResult]
}

object ValidateResource {

  def apply(
      schemaClaimResolver: SchemaClaimResolver,
      validateShacl: ValidateShacl
  ): ValidateResource =
    new ValidateResource {
      override def apply(
          jsonld: JsonLdAssembly,
          schemaClaim: SchemaClaim,
          enforceSchema: Boolean
      ): IO[ValidationResult] = {
        assertNotReservedId(jsonld.id) >>
          assertNotReservedTypes(jsonld.types) >>
          schemaClaimResolver(schemaClaim, jsonld.types, enforceSchema).flatMap {
            case Some(schema) => apply(jsonld, schema)
            case None         => IO.pure(NoValidation(schemaClaim.project))
          }
      }

      def apply(jsonld: JsonLdAssembly, schema: ResourceF[Schema]): IO[ValidationResult] = {
        val schemaRef = ResourceRef.Revision(schema.id, schema.rev)
        for {
          report <- shaclValidate(jsonld, schemaRef, schema)
          _      <- IO.raiseUnless(report.conforms)(InvalidResource(jsonld.id, schemaRef, report, jsonld.expanded))
          _      <- IO.raiseUnless(report.withTargetedNodes)(NoTargetedNode(jsonld.id, schemaRef, jsonld.expanded))
        } yield Validated(schema.value.project, ResourceRef.Revision(schema.id, schema.rev), report)
      }

      private def shaclValidate(
          jsonld: JsonLdAssembly,
          schemaRef: ResourceRef,
          schema: ResourceF[Schema]
      ): IO[ValidationReport] = {
        for {
          ontologies <- schema.value.ontologies
          shapes     <- schema.value.shapes
          report     <- validateShacl(jsonld.graph ++ ontologies, shapes, reportDetails = true)
        } yield report
      }.adaptError { e =>
        ResourceShaclEngineRejection(jsonld.id, schemaRef, e)
      }.span("validateShacl")

      private def assertNotReservedId(resourceId: Iri) = {
        IO.raiseWhen(resourceId.startsWith(contexts.base))(ReservedResourceId(resourceId))
      }

      private def assertNotReservedTypes(types: Set[Iri]) = {
        IO.raiseWhen(types.exists(_.startsWith(Vocabulary.nxv.base)))(ReservedResourceTypes(types))
      }
    }
}
