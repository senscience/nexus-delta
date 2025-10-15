package ai.senscience.nexus.delta.wiring

import ai.senscience.nexus.delta.Main.pluginsMaxPriority
import ai.senscience.nexus.delta.kernel.utils.{ClasspathResourceLoader, UUIDF}
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.shacl.ValidateShacl
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.routes.{SchemaJobRoutes, SchemasRoutes}
import ai.senscience.nexus.delta.sdk.*
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.directives.DeltaSchemeDirectives
import ai.senscience.nexus.delta.sdk.fusion.FusionConfig
import ai.senscience.nexus.delta.sdk.identities.Identities
import ai.senscience.nexus.delta.sdk.model.*
import ai.senscience.nexus.delta.sdk.projects.FetchContext
import ai.senscience.nexus.delta.sdk.projects.model.ApiMappings
import ai.senscience.nexus.delta.sdk.resolvers.{ResolverContextResolution, Resolvers}
import ai.senscience.nexus.delta.sdk.resources.{FetchResource, Resources, ValidateResource}
import ai.senscience.nexus.delta.sdk.schemas.*
import ai.senscience.nexus.delta.sdk.schemas.Schemas.{SchemaDefinition, SchemaLog}
import ai.senscience.nexus.delta.sdk.schemas.job.{SchemaValidationCoordinator, SchemaValidationStream}
import ai.senscience.nexus.delta.sdk.schemas.model.{Schema, SchemaEvent}
import ai.senscience.nexus.delta.sdk.sse.SseEncoder
import ai.senscience.nexus.delta.sdk.wiring.NexusModuleDef
import ai.senscience.nexus.delta.sourcing.projections.{ProjectionErrors, Projections}
import ai.senscience.nexus.delta.sourcing.stream.Supervisor
import ai.senscience.nexus.delta.sourcing.{ScopedEventLog, Transactors}
import cats.effect.{Clock, IO}
import izumi.distage.model.definition.Id
import org.typelevel.otel4s.trace.Tracer

/**
  * Schemas wiring
  */
object SchemasModule extends NexusModuleDef {
  private given ClasspathResourceLoader = ClasspathResourceLoader.withContext(getClass)

  makeConfig[SchemasConfig]("app.schemas")

  makeTracer("schemas")

  addRemoteContextResolution(contexts.definition)

  make[ValidateShacl].fromEffect { (rcr: RemoteContextResolution @Id("aggregate")) => ValidateShacl(rcr) }

  make[ValidateSchema].from {
    (
        validateShacl: ValidateShacl,
        tracer: Tracer[IO] @Id("schemas")
    ) =>
      ValidateSchema(validateShacl)(using tracer)
  }

  make[SchemaDefinition].from { (validateSchema: ValidateSchema, clock: Clock[IO]) =>
    Schemas.definition(validateSchema, clock)
  }

  make[SchemaLog].from { (scopedDefinition: SchemaDefinition, config: SchemasConfig, xas: Transactors) =>
    ScopedEventLog(scopedDefinition, config.eventLog, xas)
  }

  make[FetchSchema].from { (schemaLog: SchemaLog) =>
    FetchSchema(schemaLog)
  }

  make[Schemas].from {
    (
        schemaLog: SchemaLog,
        fetchContext: FetchContext,
        schemaImports: SchemaImports,
        resolverContextResolution: ResolverContextResolution,
        uuidF: UUIDF,
        tracer: Tracer[IO] @Id("schemas")
    ) =>
      SchemasImpl(
        schemaLog,
        fetchContext,
        schemaImports,
        resolverContextResolution
      )(using uuidF)(using tracer)
  }

  make[SchemaImports].from {
    (
        aclCheck: AclCheck,
        resolvers: Resolvers,
        fetchSchema: FetchSchema,
        fetchResource: FetchResource
    ) =>
      SchemaImports(aclCheck, resolvers, fetchSchema, fetchResource)
  }

  make[SchemaValidationStream].fromEffect {
    (resources: Resources, fetchSchema: FetchSchema, validateResource: ValidateResource, config: SchemasConfig) =>
      FetchSchema.cached(fetchSchema, config.cache).map { cached =>
        SchemaValidationStream(
          resources.currentStates,
          cached,
          validateResource
        )
      }

  }

  make[SchemaValidationCoordinator].from { (supervisor: Supervisor, schemaValidationStream: SchemaValidationStream) =>
    SchemaValidationCoordinator(supervisor, schemaValidationStream)
  }

  make[SchemasRoutes].from {
    (
        identities: Identities,
        aclCheck: AclCheck,
        schemas: Schemas,
        schemeDirectives: DeltaSchemeDirectives,
        baseUri: BaseUri,
        cr: RemoteContextResolution @Id("aggregate"),
        ordering: JsonKeyOrdering,
        fusionConfig: FusionConfig
    ) =>
      new SchemasRoutes(identities, aclCheck, schemas, schemeDirectives)(
        baseUri,
        cr,
        ordering,
        fusionConfig
      )
  }

  make[SchemaJobRoutes].from {
    (
        identities: Identities,
        aclCheck: AclCheck,
        fetchContext: FetchContext,
        schemaValidationCoordinator: SchemaValidationCoordinator,
        projections: Projections,
        projectionsErrors: ProjectionErrors,
        baseUri: BaseUri,
        cr: RemoteContextResolution @Id("aggregate"),
        ordering: JsonKeyOrdering
    ) =>
      new SchemaJobRoutes(
        identities,
        aclCheck,
        fetchContext,
        schemaValidationCoordinator,
        projections,
        projectionsErrors
      )(
        baseUri,
        cr,
        ordering
      )
  }

  many[SseEncoder[?]].add { (base: BaseUri) => SchemaEvent.sseEncoder(base) }

  many[ApiMappings].add(Schemas.mappings)

  many[MetadataContextValue].addEffect(MetadataContextValue.fromFile("contexts/schemas-metadata.json"))

  many[PriorityRoute].add { (route: SchemasRoutes) =>
    PriorityRoute(pluginsMaxPriority + 8, route.routes, requiresStrictEntity = true)
  }

  many[PriorityRoute].add { (route: SchemaJobRoutes) =>
    PriorityRoute(pluginsMaxPriority + 8, route.routes, requiresStrictEntity = true)
  }

  make[Schema.Shift].from { (schemas: Schemas, base: BaseUri) =>
    Schema.shift(schemas)(base)
  }

  many[ResourceShift[?, ?]].ref[Schema.Shift]
}
