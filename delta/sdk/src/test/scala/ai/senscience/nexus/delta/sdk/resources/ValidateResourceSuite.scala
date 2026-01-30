package ai.senscience.nexus.delta.sdk.resources

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.{contexts, nxv, schemas}
import ai.senscience.nexus.delta.rdf.jsonld.api.{JsonLdApi, TitaniumJsonLdApi}
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.jsonld.{CompactedJsonLd, ExpandedJsonLd}
import ai.senscience.nexus.delta.rdf.shacl.ValidateShacl
import ai.senscience.nexus.delta.sdk.RemoteContextResolutionFixtures
import ai.senscience.nexus.delta.sdk.generators.{ResourceResolutionGen, SchemaGen}
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.jsonld.JsonLdAssembly
import ai.senscience.nexus.delta.sdk.model.Fetch.FetchF
import ai.senscience.nexus.delta.sdk.resolvers.ResolverResolution.ResourceResolution
import ai.senscience.nexus.delta.sdk.resolvers.model.ResourceResolutionReport.ResolverReport
import ai.senscience.nexus.delta.sdk.resolvers.model.{ResolverResolutionRejection, ResourceResolutionReport}
import ai.senscience.nexus.delta.sdk.resources.ResourcesConfig.SchemaEnforcementConfig
import ai.senscience.nexus.delta.sdk.resources.model.ResourceRejection.*
import ai.senscience.nexus.delta.sdk.schemas.model.Schema
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.ResourceRef.Latest
import ai.senscience.nexus.delta.sourcing.model.{Identity, Label, ProjectRef, ResourceRef}
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.effect.IO
import io.circe.Json
import munit.Location

class ValidateResourceSuite extends NexusSuite with RemoteContextResolutionFixtures {

  private given JsonLdApi                    = TitaniumJsonLdApi.lenient
  private given rcr: RemoteContextResolution = loadCoreContextsAndSchemas

  private val project = ProjectRef.unsafe("org", "proj")

  private val subject: Subject = Identity.User("user", Label.unsafe("realm"))
  private val caller: Caller   = Caller(subject, Set(subject))

  private def schemaValue(id: Iri) = loader
    .jsonContentOf("resources/schema.json")
    .map(_.addContext(contexts.shacl, contexts.schemasMetadata))
    .flatMap { source =>
      SchemaGen.schemaAsync(id, project, source)
    }

  private val schemaId  = nxv + "my-schema"
  private val schemaRef = ResourceRef.Revision(schemaId, 1)
  private val schema    = schemaValue(schemaId)
    .map(SchemaGen.resourceFor(_))

  private val unconstrained = ResourceRef.Revision(schemas.resources, 1)

  private val fetchSchema: (ResourceRef, ProjectRef) => FetchF[Schema] = {
    case (ref, p) if ref.iri == schemaId && p == project => schema.map(Some(_))
    case _                                               => IO.none
  }
  private val schemaResolution: ResourceResolution[Schema]             =
    ResourceResolutionGen.singleInProject(project, fetchSchema)

  private def sourceWithId(id: Iri, patch: Json => Json) =
    loader.jsonContentOf("resources/resource.json", "id" -> id).map(patch)

  private def jsonLdWithId(id: Iri, patchSource: Json => Json): IO[JsonLdAssembly] = {
    for {
      patchedSource <- sourceWithId(id, patchSource)
      expanded      <- ExpandedJsonLd(patchedSource)
      graph         <- expanded.toGraph
    } yield JsonLdAssembly(id, patchedSource, CompactedJsonLd.empty, expanded, graph, Set.empty)
  }

  private val validResourceId = nxv + "valid"
  private val validResource   = jsonLdWithId(validResourceId, identity)

  private val schemaEnforcementConfig = SchemaEnforcementConfig(Set.empty, allowNoTypes = false)
  private val schemaClaimResolver     = SchemaClaimResolver(schemaResolution, schemaEnforcementConfig)

  private val validateResource = ValidateResource(schemaClaimResolver, ValidateShacl(rcr).accepted)

  private def assertResult(result: ValidationResult, expectedProject: ProjectRef, expectedSchema: ResourceRef.Revision)(
      using Location
  ): Unit = {
    assertEquals(result.project, expectedProject)
    assertEquals(result.schema, expectedSchema)
  }

  test("Validate a resource with the appropriate schema") {
    for {
      jsonLd     <- validResource
      schemaClaim = SchemaClaim.onCreate(project, schemaRef, caller)
      result     <- validateResource(jsonLd, schemaClaim, enforceSchema = false)
    } yield {
      assertResult(result, project, schemaRef)
    }
  }

  test("Validate a resource with no schema and no schema enforcement is enabled") {
    for {
      jsonLd     <- validResource
      schemaClaim = SchemaClaim.onCreate(project, unconstrained, caller)
      result     <- validateResource(jsonLd, schemaClaim, enforceSchema = false)
    } yield {
      assertResult(result, project, unconstrained)
    }
  }

  test("Reject a resource when the id starts with a reserved prefix") {
    val id = contexts.base / "fail"
    for {
      jsonLd     <- jsonLdWithId(id, identity)
      schemaClaim = SchemaClaim.onCreate(project, schemaRef, caller)
      _          <- validateResource(jsonLd, schemaClaim, enforceSchema = false)
                      .interceptEquals(ReservedResourceId(id))
    } yield ()
  }

  test("Reject a resource when one type is starting with the Nexus vocabulary") {
    val forbiddenType        = nxv + "Forbidden"
    val forbiddenTypePayload = json"""{ "@type": "$forbiddenType" } """
    for {
      jsonLd     <- jsonLdWithId(validResourceId, _.deepMerge(forbiddenTypePayload))
      schemaClaim = SchemaClaim.onCreate(project, schemaRef, caller)
      _          <- validateResource(jsonLd, schemaClaim, enforceSchema = false)
                      .interceptEquals(ReservedResourceTypes(Set(forbiddenType)))
    } yield ()
  }

  test("Reject a resource with no schema and schema enforcement is enabled") {
    for {
      jsonLd     <- validResource
      schemaClaim = SchemaClaim.onCreate(project, unconstrained, caller)
      _          <- validateResource(jsonLd, schemaClaim, enforceSchema = true).interceptEquals(SchemaIsMandatory(project))
    } yield ()
  }

  test("Reject a resource when schema is not found") {
    val unknownSchema = Latest(nxv + "not-found")
    val expectedError = InvalidSchemaRejection(
      unknownSchema,
      project,
      ResourceResolutionReport(
        ResolverReport.failed(
          nxv + "in-project",
          project -> ResolverResolutionRejection.ResourceNotFound(unknownSchema.iri, project)
        )
      )
    )
    for {
      jsonLd     <- validResource
      schemaClaim = SchemaClaim.onCreate(project, unknownSchema, caller)
      _          <- validateResource(jsonLd, schemaClaim, enforceSchema = true).interceptEquals(expectedError)
    } yield ()
  }

  test("Reject a resource when it can't be validated by the provided schema") {
    for {
      jsonLd     <- jsonLdWithId(validResourceId, _.removeKeys("name"))
      schemaClaim = SchemaClaim.onCreate(project, schemaRef, caller)
      _          <- validateResource(jsonLd, schemaClaim, enforceSchema = true).intercept[InvalidResource]
    } yield ()
  }

  test("Reject a resource when it can't be targeted by the provided schema") {
    for {
      jsonLd     <- jsonLdWithId(validResourceId, _.replaceKeyWithValue("@type", "nxv:Another"))
      schemaClaim = SchemaClaim.onCreate(project, schemaRef, caller)
      _          <- validateResource(jsonLd, schemaClaim, enforceSchema = true).intercept[NoTargetedNode]
    } yield ()
  }

}
