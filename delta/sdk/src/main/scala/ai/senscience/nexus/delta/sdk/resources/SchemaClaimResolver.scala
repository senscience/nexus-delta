package ai.senscience.nexus.delta.sdk.resources

import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.model.ResourceF
import ai.senscience.nexus.delta.sdk.resolvers.ResolverResolution.ResourceResolution
import ai.senscience.nexus.delta.sdk.resources.ResourcesConfig.SchemaEnforcementConfig
import ai.senscience.nexus.delta.sdk.resources.SchemaClaim.*
import ai.senscience.nexus.delta.sdk.resources.model.ResourceRejection.{InvalidSchemaRejection, SchemaIsDeprecated, SchemaIsMandatory}
import ai.senscience.nexus.delta.sdk.schemas.model.Schema
import cats.effect.IO
import cats.syntax.all.*
import ch.epfl.bluebrain.nexus.delta.rdf.IriOrBNode.Iri
import ch.epfl.bluebrain.nexus.delta.sourcing.model.{ProjectRef, ResourceRef}

trait SchemaClaimResolver {

  /**
    * Resolves the schema claim to an actual schema
    * @return
    *   the schema or none if unconstrained resources is allowed
    */
  def apply(schemaClaim: SchemaClaim, resourceTypes: Set[Iri], enforceSchema: Boolean): IO[Option[ResourceF[Schema]]]

}

object SchemaClaimResolver {

  def apply(
      resourceResolution: ResourceResolution[Schema],
      schemaEnforcement: SchemaEnforcementConfig
  ): SchemaClaimResolver = new SchemaClaimResolver {
    override def apply(
        schemaClaim: SchemaClaim,
        resourceTypes: Set[Iri],
        enforceSchema: Boolean
    ): IO[Option[ResourceF[Schema]]] =
      schemaClaim match {
        case CreateWithSchema(project, schema, caller) =>
          resolveSchema(project, schema, caller)
        case CreateUnconstrained(project)              =>
          onUnconstrained(project, resourceTypes, enforceSchema)
        case UpdateToSchema(project, schema, caller)   =>
          resolveSchema(project, schema, caller)
        case UpdateToUnconstrained(project)            =>
          onUnconstrained(project, resourceTypes, enforceSchema)
        case KeepUnconstrained(_)                      =>
          IO.none
      }

    private def assertNotDeprecated(schema: ResourceF[Schema]) = {
      IO.raiseWhen(schema.deprecated)(SchemaIsDeprecated(schema.value.id))
    }

    private def resolveSchema(project: ProjectRef, schema: ResourceRef, caller: Caller) = {
      resourceResolution
        .resolve(schema, project)(caller)
        .flatMap { result =>
          val invalidSchema = result.leftMap(InvalidSchemaRejection(schema, project, _))
          IO.fromEither(invalidSchema)
        }
        .flatTap(schema => assertNotDeprecated(schema))
        .map(Some(_))
    }

    private def onUnconstrained(project: ProjectRef, resourceTypes: Set[Iri], enforceSchema: Boolean) = {
      val enforcementRequired = enforceSchema && (
        resourceTypes.isEmpty && !schemaEnforcement.allowNoTypes ||
          resourceTypes.nonEmpty && !resourceTypes.forall(schemaEnforcement.typeWhitelist.contains)
      )
      IO.raiseWhen(enforcementRequired)(SchemaIsMandatory(project)).as(None)
    }
  }

}
