package ai.senscience.nexus.delta.sdk.resolvers

import ai.senscience.nexus.delta.kernel.utils.UUIDF
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.contexts
import ai.senscience.nexus.delta.sdk.*
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sdk.jsonld.JsonLdSourceProcessor.JsonLdSourceResolvingDecoder
import ai.senscience.nexus.delta.sdk.model.IdSegmentRef.{Latest, Revision, Tag}
import ai.senscience.nexus.delta.sdk.model.search.SearchResults
import ai.senscience.nexus.delta.sdk.model.search.SearchResults.UnscoredSearchResults
import ai.senscience.nexus.delta.sdk.model.{IdSegment, IdSegmentRef}
import ai.senscience.nexus.delta.sdk.projects.FetchContext
import ai.senscience.nexus.delta.sdk.resolvers.Resolvers.expandIri
import ai.senscience.nexus.delta.sdk.resolvers.ResolversImpl.ResolversLog
import ai.senscience.nexus.delta.sdk.resolvers.model.*
import ai.senscience.nexus.delta.sdk.resolvers.model.ResolverCommand.{CreateResolver, DeprecateResolver, UpdateResolver}
import ai.senscience.nexus.delta.sdk.resolvers.model.ResolverRejection.{FetchByTagNotSupported, ResolverNotFound, RevisionNotFound}
import ai.senscience.nexus.delta.sourcing.config.EventLogConfig
import ai.senscience.nexus.delta.sourcing.model.{Identity, ProjectRef}
import ai.senscience.nexus.delta.sourcing.{Scope, ScopedEventLog, Transactors}
import cats.effect.{Clock, IO}
import io.circe.Json
import org.typelevel.otel4s.trace.Tracer

final class ResolversImpl private (
    log: ResolversLog,
    fetchContext: FetchContext,
    sourceDecoder: JsonLdSourceResolvingDecoder[ResolverValue]
)(using Tracer[IO])
    extends Resolvers {

  override def create(
      projectRef: ProjectRef,
      source: Json
  )(using caller: Caller): IO[ResolverResource] = {
    for {
      pc                   <- fetchContext.onCreate(projectRef)
      (iri, resolverValue) <- sourceDecoder(projectRef, pc, source)
      res                  <- eval(CreateResolver(iri, projectRef, resolverValue, source, caller))
    } yield res
  }.surround("createResolver")

  override def create(
      id: IdSegment,
      projectRef: ProjectRef,
      source: Json
  )(using caller: Caller): IO[ResolverResource] = {
    for {
      pc            <- fetchContext.onCreate(projectRef)
      iri           <- expandIri(id, pc)
      resolverValue <- sourceDecoder(projectRef, pc, iri, source)
      res           <- eval(CreateResolver(iri, projectRef, resolverValue, source, caller))
    } yield res
  }.surround("createResolver")

  override def create(
      id: IdSegment,
      projectRef: ProjectRef,
      resolverValue: ResolverValue
  )(using caller: Caller): IO[ResolverResource] = {
    for {
      pc    <- fetchContext.onCreate(projectRef)
      iri   <- expandIri(id, pc)
      source = ResolverValue.generateSource(iri, resolverValue)
      res   <- eval(CreateResolver(iri, projectRef, resolverValue, source, caller))
    } yield res
  }.surround("createResolver")

  override def update(
      id: IdSegment,
      projectRef: ProjectRef,
      rev: Int,
      source: Json
  )(using caller: Caller): IO[ResolverResource] = {
    for {
      pc            <- fetchContext.onModify(projectRef)
      iri           <- expandIri(id, pc)
      resolverValue <- sourceDecoder(projectRef, pc, iri, source)
      res           <- eval(UpdateResolver(iri, projectRef, resolverValue, source, rev, caller))
    } yield res
  }.surround("updateResolver")

  override def update(
      id: IdSegment,
      projectRef: ProjectRef,
      rev: Int,
      resolverValue: ResolverValue
  )(using caller: Caller): IO[ResolverResource] = {
    for {
      pc    <- fetchContext.onModify(projectRef)
      iri   <- expandIri(id, pc)
      source = ResolverValue.generateSource(iri, resolverValue)
      res   <- eval(UpdateResolver(iri, projectRef, resolverValue, source, rev, caller))
    } yield res
  }.surround("updateResolver")

  override def deprecate(
      id: IdSegment,
      projectRef: ProjectRef,
      rev: Int
  )(using subject: Identity.Subject): IO[ResolverResource] = {
    for {
      pc  <- fetchContext.onModify(projectRef)
      iri <- expandIri(id, pc)
      res <- eval(DeprecateResolver(iri, projectRef, rev, subject))
    } yield res
  }.surround("deprecateResolver")

  override def fetch(id: IdSegmentRef, projectRef: ProjectRef): IO[ResolverResource] = {
    for {
      pc      <- fetchContext.onRead(projectRef)
      iri     <- expandIri(id.value, pc)
      notFound = ResolverNotFound(iri, projectRef)
      state   <- id match {
                   case Latest(_)        => log.stateOr(projectRef, iri, notFound)
                   case Revision(_, rev) =>
                     log.stateOr(projectRef, iri, rev, notFound, RevisionNotFound(_, _))
                   case Tag(_, tag)      =>
                     log.stateOr(projectRef, iri, tag, notFound, FetchByTagNotSupported(tag))
                 }
    } yield state.toResource
  }.surround("fetchResolver")

  def list(project: ProjectRef): IO[UnscoredSearchResults[ResolverResource]] =
    SearchResults(
      log.currentStates(Scope.Project(project), _.toResource)
    ).surround("listResolvers")

  private def eval(cmd: ResolverCommand): IO[ResolverResource] =
    log.evaluate(cmd.project, cmd.id, cmd).map(_.state.toResource)
}

object ResolversImpl {

  type ResolversLog = ScopedEventLog[Iri, ResolverState, ResolverCommand, ResolverEvent, ResolverRejection]

  /**
    * Constructs a Resolver instance
    */
  def apply(
      fetchContext: FetchContext,
      contextResolution: ResolverContextResolution,
      validatePriority: ValidatePriority,
      config: EventLogConfig,
      xas: Transactors,
      clock: Clock[IO]
  )(using uuidF: UUIDF)(using Tracer[IO]): Resolvers = {
    new ResolversImpl(
      ScopedEventLog(Resolvers.definition(validatePriority, clock), config, xas),
      fetchContext,
      new JsonLdSourceResolvingDecoder[ResolverValue](
        contexts.resolvers,
        contextResolution,
        uuidF
      )
    )
  }
}
