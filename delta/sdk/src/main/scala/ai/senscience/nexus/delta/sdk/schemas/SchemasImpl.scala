package ai.senscience.nexus.delta.sdk.schemas

import ai.senscience.nexus.delta.kernel.utils.UUIDF
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.{contexts, nxv}
import ai.senscience.nexus.delta.rdf.jsonld.ExpandedJsonLd
import ai.senscience.nexus.delta.sdk.*
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sdk.jsonld.JsonLdSourceProcessor.JsonLdSourceResolvingParser
import ai.senscience.nexus.delta.sdk.model.*
import ai.senscience.nexus.delta.sdk.model.search.SearchResults
import ai.senscience.nexus.delta.sdk.model.search.SearchResults.UnscoredSearchResults
import ai.senscience.nexus.delta.sdk.projects.FetchContext
import ai.senscience.nexus.delta.sdk.resolvers.ResolverContextResolution
import ai.senscience.nexus.delta.sdk.schemas.Schemas.{expandIri, SchemaLog}
import ai.senscience.nexus.delta.sdk.schemas.SchemasImpl.SchemasLog
import ai.senscience.nexus.delta.sdk.schemas.model.SchemaCommand.*
import ai.senscience.nexus.delta.sdk.schemas.model.SchemaRejection.SchemaNotFound
import ai.senscience.nexus.delta.sdk.schemas.model.{SchemaCommand, SchemaEvent, SchemaRejection, SchemaState}
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.model.Tag.UserTag
import ai.senscience.nexus.delta.sourcing.{Scope, ScopedEventLog}
import cats.effect.IO
import cats.syntax.all.*
import io.circe.Json
import org.typelevel.otel4s.trace.Tracer

final class SchemasImpl private (
    log: SchemasLog,
    fetchContext: FetchContext,
    schemaImports: SchemaImports,
    sourceParser: JsonLdSourceResolvingParser
)(using Tracer[IO])
    extends Schemas {

  override def create(projectRef: ProjectRef, source: Json)(using Caller): IO[SchemaResource] =
    createCommand(None, projectRef, source).flatMap(eval).surround("createSchema")

  override def create(id: IdSegment, projectRef: ProjectRef, source: Json)(using Caller): IO[SchemaResource] =
    createCommand(Some(id), projectRef, source).flatMap(eval).surround("createSchema")

  override def createDryRun(projectRef: ProjectRef, source: Json)(using Caller): IO[SchemaResource] =
    createCommand(None, projectRef, source).flatMap(dryRun)

  private def createCommand(id: Option[IdSegment], projectRef: ProjectRef, source: Json)(using
      caller: Caller
  ): IO[CreateSchema] =
    for {
      pc               <- fetchContext.onCreate(projectRef)
      iri              <- id.traverse(expandIri(_, pc))
      jsonLd           <- sourceParser(projectRef, pc, iri, source)
      expandedResolved <- resolveImports(jsonLd.id, projectRef, jsonLd.expanded)
    } yield CreateSchema(jsonLd.id, projectRef, source, jsonLd.compacted, expandedResolved, caller.subject)

  override def update(id: IdSegment, projectRef: ProjectRef, rev: Int, source: Json)(using
      caller: Caller
  ): IO[SchemaResource] = {
    for {
      pc                    <- fetchContext.onModify(projectRef)
      iri                   <- expandIri(id, pc)
      (compacted, expanded) <- sourceParser(projectRef, pc, iri, source).map { j => (j.compacted, j.expanded) }
      expandedResolved      <- resolveImports(iri, projectRef, expanded)
      res                   <-
        eval(UpdateSchema(iri, projectRef, source, compacted, expandedResolved, rev, caller.subject))
    } yield res
  }.surround("updateSchema")

  override def refresh(id: IdSegment, projectRef: ProjectRef)(using caller: Caller): IO[SchemaResource] = {
    for {
      pc                    <- fetchContext.onModify(projectRef)
      iri                   <- expandIri(id, pc)
      schema                <- log.stateOr(projectRef, iri, SchemaNotFound(iri, projectRef))
      (compacted, expanded) <- sourceParser(projectRef, pc, iri, schema.source).map { j => (j.compacted, j.expanded) }
      expandedResolved      <- resolveImports(iri, projectRef, expanded)
      res                   <-
        eval(RefreshSchema(iri, projectRef, compacted, expandedResolved, schema.rev, caller.subject))
    } yield res
  }.surround("refreshSchema")

  override def tag(
      id: IdSegment,
      projectRef: ProjectRef,
      tag: UserTag,
      tagRev: Int,
      rev: Int
  )(using caller: Subject): IO[SchemaResource] = {
    for {
      pc  <- fetchContext.onModify(projectRef)
      iri <- expandIri(id, pc)
      res <- eval(TagSchema(iri, projectRef, tagRev, tag, rev, caller))
    } yield res
  }.surround("tagSchema")

  override def deleteTag(
      id: IdSegment,
      projectRef: ProjectRef,
      tag: UserTag,
      rev: Int
  )(using caller: Subject): IO[SchemaResource] =
    (for {
      pc  <- fetchContext.onModify(projectRef)
      iri <- expandIri(id, pc)
      res <- eval(DeleteSchemaTag(iri, projectRef, tag, rev, caller))
    } yield res).surround("deleteSchemaTag")

  override def deprecate(
      id: IdSegment,
      projectRef: ProjectRef,
      rev: Int
  )(using caller: Subject): IO[SchemaResource] =
    (for {
      pc  <- fetchContext.onModify(projectRef)
      iri <- expandIri(id, pc)
      res <- eval(DeprecateSchema(iri, projectRef, rev, caller))
    } yield res).surround("deprecateSchema")

  override def undeprecate(
      id: IdSegment,
      projectRef: ProjectRef,
      rev: Int
  )(using caller: Subject): IO[SchemaResource] =
    (for {
      pc  <- fetchContext.onModify(projectRef)
      iri <- expandIri(id, pc)
      res <- eval(UndeprecateSchema(iri, projectRef, rev, caller))
    } yield res).surround("undeprecateSchema")

  override def fetch(id: IdSegmentRef, projectRef: ProjectRef): IO[SchemaResource] = {
    for {
      pc       <- fetchContext.onRead(projectRef)
      ref      <- expandIri(id, pc)
      resource <- FetchSchema(log)(ref, projectRef)
    } yield resource
  }.surround("fetchSchema")

  private def eval(cmd: SchemaCommand) =
    log
      .evaluate(cmd.project, cmd.id, cmd)
      .map(_.state.toResource)
      .surround("evalSchemaCommand")

  private def dryRun(cmd: SchemaCommand) =
    log.dryRun(cmd.project, cmd.id, cmd).map(_.state.toResource)

  private def resolveImports(id: Iri, projectRef: ProjectRef, expanded: ExpandedJsonLd)(using Caller) =
    schemaImports.resolve(id, projectRef, expanded.addType(nxv.Schema))

  def list(project: ProjectRef): IO[UnscoredSearchResults[SchemaResource]] =
    log
      .currentStates(Scope.Project(project), _.toResource)
      .compile
      .toList
      .map { results => SearchResults(results.size.toLong, results) }
      .surround("listSchemas")
}

object SchemasImpl {

  type SchemasLog = ScopedEventLog[Iri, SchemaState, SchemaCommand, SchemaEvent, SchemaRejection]

  /**
    * Constructs a [[Schemas]] instance.
    */
  final def apply(
      scopedLog: SchemaLog,
      fetchContext: FetchContext,
      schemaImports: SchemaImports,
      contextResolution: ResolverContextResolution
  )(using uuidF: UUIDF)(using Tracer[IO]): Schemas = {
    val parser =
      new JsonLdSourceResolvingParser(
        List(contexts.shacl, contexts.schemasMetadata),
        contextResolution,
        uuidF
      )
    new SchemasImpl(
      scopedLog,
      fetchContext,
      schemaImports,
      parser
    )
  }

}
