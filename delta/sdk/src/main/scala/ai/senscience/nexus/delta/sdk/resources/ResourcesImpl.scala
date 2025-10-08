package ai.senscience.nexus.delta.sdk.resources

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.kernel.utils.UUIDF
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sdk.*
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sdk.jsonld.JsonLdSourceProcessor.JsonLdSourceResolvingParser
import ai.senscience.nexus.delta.sdk.model.*
import ai.senscience.nexus.delta.sdk.projects.FetchContext
import ai.senscience.nexus.delta.sdk.projects.model.ProjectContext
import ai.senscience.nexus.delta.sdk.resolvers.ResolverContextResolution
import ai.senscience.nexus.delta.sdk.resources.Resources.{expandIri, expandResourceRef, ResourceLog}
import ai.senscience.nexus.delta.sdk.resources.ResourcesImpl.{logger, ResourcesLog}
import ai.senscience.nexus.delta.sdk.resources.model.ResourceCommand.*
import ai.senscience.nexus.delta.sdk.resources.model.ResourceRejection.{NoChangeDetected, ResourceNotFound}
import ai.senscience.nexus.delta.sdk.resources.model.{ResourceCommand, ResourceEvent, ResourceRejection, ResourceState}
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.model.Tag.UserTag
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.stream.SuccessElemStream
import ai.senscience.nexus.delta.sourcing.{Scope, ScopedEventLog}
import cats.effect.IO
import io.circe.Json
import org.typelevel.otel4s.trace.Tracer

final class ResourcesImpl private (
    log: ResourcesLog,
    fetchContext: FetchContext,
    sourceParser: JsonLdSourceResolvingParser
)(using Tracer[IO])
    extends Resources {

  override def create(
      projectRef: ProjectRef,
      schema: IdSegment,
      source: Json,
      tag: Option[UserTag]
  )(implicit caller: Caller): IO[DataResource] = {
    for {
      projectContext <- fetchContext.onCreate(projectRef)
      schemeRef      <- IO.fromEither(expandResourceRef(schema, projectContext))
      jsonld         <- sourceParser(projectRef, projectContext, source)
      res            <- eval(CreateResource(projectRef, projectContext, schemeRef, jsonld, caller, tag))
    } yield res
  }.surround("createResource")

  override def create(
      id: IdSegment,
      projectRef: ProjectRef,
      schema: IdSegment,
      source: Json,
      tag: Option[UserTag]
  )(implicit caller: Caller): IO[DataResource] = {
    for {
      (iri, projectContext) <- expandWithContext(fetchContext.onCreate, projectRef, id)
      schemeRef             <- IO.fromEither(expandResourceRef(schema, projectContext))
      jsonld                <- sourceParser(projectRef, projectContext, iri, source)
      res                   <- eval(CreateResource(projectRef, projectContext, schemeRef, jsonld, caller, tag))
    } yield res
  }.surround("createResource")

  override def update(
      id: IdSegment,
      projectRef: ProjectRef,
      schemaOpt: Option[IdSegment],
      rev: Int,
      source: Json,
      tag: Option[UserTag]
  )(implicit caller: Caller): IO[DataResource] = {
    for {
      (iri, projectContext) <- expandWithContext(fetchContext.onModify, projectRef, id)
      schemeRefOpt          <- IO.fromEither(expandResourceRef(schemaOpt, projectContext))
      jsonld                <- sourceParser(projectRef, projectContext, iri, source)
      res                   <- eval(UpdateResource(projectRef, projectContext, schemeRefOpt, jsonld, rev, caller, tag))
    } yield res
  }.surround("updateResource")

  override def updateAttachedSchema(
      id: IdSegment,
      projectRef: ProjectRef,
      schema: IdSegment
  )(implicit caller: Caller): IO[DataResource] = {
    for {
      (iri, projectContext) <- expandWithContext(fetchContext.onModify, projectRef, id)
      schemaRef             <- IO.fromEither(expandResourceRef(schema, projectContext))
      resource              <- log.stateOr(projectRef, iri, ResourceNotFound(iri, projectRef))
      res                   <- eval(UpdateResourceSchema(iri, projectRef, projectContext, schemaRef, resource.rev, caller))
    } yield res
  }.surround("updateResourceSchema")

  override def refresh(
      id: IdSegment,
      projectRef: ProjectRef,
      schemaOpt: Option[IdSegment]
  )(implicit caller: Caller): IO[DataResource] = {
    for {
      (iri, projectContext) <- expandWithContext(fetchContext.onModify, projectRef, id)
      schemaRefOpt          <- IO.fromEither(expandResourceRef(schemaOpt, projectContext))
      resource              <- log.stateOr(projectRef, iri, ResourceNotFound(iri, projectRef))
      jsonld                <- sourceParser(projectRef, projectContext, iri, resource.source)
      res                   <- eval(RefreshResource(projectRef, projectContext, schemaRefOpt, jsonld, resource.rev, caller))
    } yield res
  }.surround("refreshResource")

  override def tag(
      id: IdSegment,
      projectRef: ProjectRef,
      schemaOpt: Option[IdSegment],
      tag: UserTag,
      tagRev: Int,
      rev: Int
  )(implicit caller: Subject): IO[DataResource] =
    (for {
      (iri, projectContext) <- expandWithContext(fetchContext.onModify, projectRef, id)
      schemeRefOpt          <- IO.fromEither(expandResourceRef(schemaOpt, projectContext))
      res                   <- eval(TagResource(iri, projectRef, schemeRefOpt, tagRev, tag, rev, caller))
    } yield res).surround("tagResource")

  override def deleteTag(
      id: IdSegment,
      projectRef: ProjectRef,
      schemaOpt: Option[IdSegment],
      tag: UserTag,
      rev: Int
  )(implicit caller: Subject): IO[DataResource] =
    (for {
      (iri, projectContext) <- expandWithContext(fetchContext.onModify, projectRef, id)
      schemeRefOpt          <- IO.fromEither(expandResourceRef(schemaOpt, projectContext))
      res                   <- eval(DeleteResourceTag(iri, projectRef, schemeRefOpt, tag, rev, caller))
    } yield res).surround("deleteResourceTag")

  override def deprecate(
      id: IdSegment,
      projectRef: ProjectRef,
      schemaOpt: Option[IdSegment],
      rev: Int
  )(implicit caller: Subject): IO[DataResource] = {
    for {
      (iri, projectContext) <- expandWithContext(fetchContext.onModify, projectRef, id)
      schemeRefOpt          <- IO.fromEither(expandResourceRef(schemaOpt, projectContext))
      res                   <- eval(DeprecateResource(iri, projectRef, schemeRefOpt, rev, caller))
    } yield res
  }.surround("deprecateResource")

  override def undeprecate(
      id: IdSegment,
      projectRef: ProjectRef,
      schemaOpt: Option[IdSegment],
      rev: Int
  )(implicit caller: Subject): IO[DataResource] = {
    for {
      (iri, projectContext) <- expandWithContext(fetchContext.onModify, projectRef, id)
      schemaRefOpt          <- IO.fromEither(expandResourceRef(schemaOpt, projectContext))
      res                   <- eval(UndeprecateResource(iri, projectRef, schemaRefOpt, rev, caller))
    } yield res
  }.surround("undeprecateResource")

  override def delete(id: IdSegment, project: ProjectRef)(implicit caller: Subject): IO[Unit] = {
    for {
      (iri, _) <- expandWithContext(fetchContext.onModify, project, id)
      _        <- logger.info(s"Deleting resource $iri in project $project")
      _        <- log.delete(project, iri, ResourceNotFound(iri, project))
    } yield ()
  }.surround("deleteResource")

  def fetchState(
      id: IdSegmentRef,
      projectRef: ProjectRef,
      schemaOpt: Option[IdSegment]
  ): IO[ResourceState] = {
    for {
      (iri, pc)    <- expandWithContext(fetchContext.onRead, projectRef, id.value)
      schemaRefOpt <- IO.fromEither(expandResourceRef(schemaOpt, pc))
      state        <- FetchResource(log).stateOrNotFound(id, iri, projectRef)
      _            <- IO.raiseWhen(schemaRefOpt.exists(_.iri != state.schema.iri))(notFound(iri, projectRef))
    } yield state
  }.surround("fetchResource")

  private def notFound(iri: Iri, ref: ProjectRef) = ResourceNotFound(iri, ref)

  override def fetch(
      id: IdSegmentRef,
      projectRef: ProjectRef,
      schemaOpt: Option[IdSegment]
  ): IO[DataResource] = fetchState(id, projectRef, schemaOpt).map(_.toResource)

  private def expandWithContext(
      fetchCtx: ProjectRef => IO[ProjectContext],
      ref: ProjectRef,
      id: IdSegment
  ): IO[(Iri, ProjectContext)] =
    fetchCtx(ref).flatMap(pc => expandIri(id, pc).map(_ -> pc))

  private def eval(cmd: ResourceCommand): IO[DataResource] =
    log.evaluate(cmd.project, cmd.id, cmd).map(_._2.toResource).recoverWith { case NoChangeDetected(currentState) =>
      val message =
        s"""Command ${cmd.getClass.getSimpleName} from '${cmd.subject}' did not result in any change on resource '${cmd.id}'
           |in project '${cmd.project}', returning the original value.""".stripMargin
      logger.info(message).as(currentState.toResource)
    }

  override def currentStates(project: ProjectRef, offset: Offset): SuccessElemStream[ResourceState] =
    log.currentStates(Scope(project), offset)
}

object ResourcesImpl {

  private val logger = Logger[ResourcesImpl]

  type ResourcesLog =
    ScopedEventLog[Iri, ResourceState, ResourceCommand, ResourceEvent, ResourceRejection]

  /**
    * Constructs a [[Resources]] instance.
    *
    * @param scopedLog
    *   the scoped resource log
    * @param fetchContext
    *   to fetch the project context
    * @param contextResolution
    *   the context resolver
    */
  final def apply(
      scopedLog: ResourceLog,
      fetchContext: FetchContext,
      contextResolution: ResolverContextResolution
  )(using uuidF: UUIDF)(using Tracer[IO]): Resources =
    new ResourcesImpl(
      scopedLog,
      fetchContext,
      JsonLdSourceResolvingParser(contextResolution, uuidF)
    )

}
