package ai.senscience.nexus.delta.elasticsearch

import ai.senscience.nexus.delta.elasticsearch.ElasticSearchViews.*
import ai.senscience.nexus.delta.elasticsearch.client.IndexLabel
import ai.senscience.nexus.delta.elasticsearch.indexing.IndexingViewDef
import ai.senscience.nexus.delta.elasticsearch.indexing.IndexingViewDef.{ActiveViewDef, DeprecatedViewDef}
import ai.senscience.nexus.delta.elasticsearch.model.*
import ai.senscience.nexus.delta.elasticsearch.model.ElasticSearchViewCommand.*
import ai.senscience.nexus.delta.elasticsearch.model.ElasticSearchViewEvent.*
import ai.senscience.nexus.delta.elasticsearch.model.ElasticSearchViewRejection.*
import ai.senscience.nexus.delta.elasticsearch.model.ElasticSearchViewType.{AggregateElasticSearch, ElasticSearch}
import ai.senscience.nexus.delta.elasticsearch.model.ElasticSearchViewValue.IndexingElasticSearchViewValue.nextIndexingRev
import ai.senscience.nexus.delta.elasticsearch.model.ElasticSearchViewValue.{AggregateElasticSearchViewValue, IndexingElasticSearchViewValue}
import ai.senscience.nexus.delta.elasticsearch.views.DefaultIndexDef
import ai.senscience.nexus.delta.kernel.kamon.KamonMetricComponent
import ai.senscience.nexus.delta.kernel.utils.UUIDF
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sdk.jsonld.ExpandIri
import ai.senscience.nexus.delta.sdk.model.*
import ai.senscience.nexus.delta.sdk.model.IdSegmentRef.{Latest, Revision, Tag}
import ai.senscience.nexus.delta.sdk.model.search.SearchResults
import ai.senscience.nexus.delta.sdk.projects.FetchContext
import ai.senscience.nexus.delta.sdk.projects.model.{ApiMappings, ProjectContext}
import ai.senscience.nexus.delta.sdk.resolvers.ResolverContextResolution
import ai.senscience.nexus.delta.sdk.views.IndexingRev
import ai.senscience.nexus.delta.sourcing.*
import ai.senscience.nexus.delta.sourcing.config.EventLogConfig
import ai.senscience.nexus.delta.sourcing.model.EntityDependency.DependsOn
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.{EntityType, ProjectRef, Tags}
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.stream.{Elem, SuccessElemStream}
import cats.effect.{Clock, IO}
import cats.syntax.all.*
import io.circe.Json

import java.util.UUID

/**
  * ElasticSearchViews resource lifecycle operations.
  */
final class ElasticSearchViews private (
    log: ElasticsearchLog,
    fetchContext: FetchContext,
    sourceDecoder: ElasticSearchViewJsonLdSourceDecoder,
    defaultViewDef: DefaultIndexDef,
    prefix: String
)(implicit uuidF: UUIDF) {

  implicit private val kamonComponent: KamonMetricComponent = KamonMetricComponent(entityType.value)

  /**
    * Creates a new ElasticSearchView with a generated id.
    *
    * @param project
    *   the parent project of the view
    * @param value
    *   the view configuration
    * @param subject
    *   the subject that initiated the action
    */
  def create(
      project: ProjectRef,
      value: ElasticSearchViewValue
  )(implicit subject: Subject): IO[ViewResource] =
    uuidF().flatMap(uuid => create(uuid.toString, project, value))

  /**
    * Creates a new ElasticSearchView with a provided id.
    *
    * @param id
    *   the id of the view either in Iri or aliased form
    * @param project
    *   the parent project of the view
    * @param value
    *   the view configuration
    * @param subject
    *   the subject that initiated the action
    */
  def create(
      id: IdSegment,
      project: ProjectRef,
      value: ElasticSearchViewValue
  )(implicit subject: Subject): IO[ViewResource] = {
    for {
      (iri, _) <- expandWithContext(fetchContext.onCreate, project, id)
      res      <- eval(CreateElasticSearchView(iri, project, value, value.toJson(iri), subject))
    } yield res
  }.span("createElasticSearchView")

  /**
    * Creates a new ElasticSearchView from a json representation. If an identifier exists in the provided json it will
    * be used; otherwise a new identifier will be generated.
    *
    * @param project
    *   the parent project of the view
    * @param source
    *   the json representation of the view
    * @param caller
    *   the caller that initiated the action
    */
  def create(
      project: ProjectRef,
      source: Json
  )(implicit caller: Caller): IO[ViewResource] = {
    for {
      pc           <- fetchContext.onCreate(project)
      (iri, value) <- sourceDecoder(project, pc, source)
      res          <- eval(CreateElasticSearchView(iri, project, value, source, caller.subject))
    } yield res
  }.span("createElasticSearchView")

  /**
    * Creates a new ElasticSearchView from a json representation. If an identifier exists in the provided json it will
    * be used as long as it matches the provided id in Iri form or as an alias; otherwise the action will be rejected.
    *
    * @param project
    *   the parent project of the view
    * @param source
    *   the json representation of the view
    * @param caller
    *   the caller that initiated the action
    */
  def create(
      id: IdSegment,
      project: ProjectRef,
      source: Json
  )(implicit caller: Caller): IO[ViewResource] = {
    for {
      (iri, pc) <- expandWithContext(fetchContext.onCreate, project, id)
      value     <- sourceDecoder(project, pc, iri, source)
      res       <- eval(CreateElasticSearchView(iri, project, value, source, caller.subject))
    } yield res
  }.span("createElasticSearchView")

  /**
    * Updates an existing ElasticSearchView.
    *
    * @param id
    *   the view identifier
    * @param project
    *   the view parent project
    * @param rev
    *   the current view revision
    * @param value
    *   the new view configuration
    * @param subject
    *   the subject that initiated the action
    */
  def update(
      id: IdSegment,
      project: ProjectRef,
      rev: Int,
      value: ElasticSearchViewValue
  )(implicit subject: Subject): IO[ViewResource] = {
    for {
      (iri, _) <- expandWithContext(fetchContext.onModify, project, id)
      res      <- eval(UpdateElasticSearchView(iri, project, rev, value, value.toJson(iri), subject))
    } yield res
  }.span("updateElasticSearchView")

  /**
    * Updates an existing ElasticSearchView.
    *
    * @param id
    *   the view identifier
    * @param project
    *   the view parent project
    * @param rev
    *   the current view revision
    * @param source
    *   the new view configuration in json representation
    * @param caller
    *   the caller that initiated the action
    */
  def update(
      id: IdSegment,
      project: ProjectRef,
      rev: Int,
      source: Json
  )(implicit caller: Caller): IO[ViewResource] = {
    for {
      (iri, pc) <- expandWithContext(fetchContext.onModify, project, id)
      _         <- validateNotDefaultView(iri)
      value     <- sourceDecoder(project, pc, iri, source)
      res       <- eval(UpdateElasticSearchView(iri, project, rev, value, source, caller.subject))
    } yield res
  }.span("updateElasticSearchView")

  /**
    * Deprecates an existing ElasticSearchView. View deprecation implies blocking any query capabilities and in case of
    * an IndexingElasticSearchView the corresponding index is deleted.
    *
    * @param id
    *   the view identifier
    * @param project
    *   the view parent project
    * @param rev
    *   the current view revision
    * @param subject
    *   the subject that initiated the action
    */
  def deprecate(
      id: IdSegment,
      project: ProjectRef,
      rev: Int
  )(implicit subject: Subject): IO[ViewResource] = {
    for {
      (iri, _) <- expandWithContext(fetchContext.onModify, project, id)
      _        <- validateNotDefaultView(iri)
      res      <- eval(DeprecateElasticSearchView(iri, project, rev, subject))
    } yield res
  }.span("deprecateElasticSearchView")

  private def validateNotDefaultView(iri: Iri): IO[Unit] =
    IO.raiseWhen(iri == defaultViewId)(ViewIsDefaultView)

  /**
    * Undeprecates an existing ElasticSearchView. View undeprecation implies unblocking any query capabilities and in
    * case of an IndexingElasticSearchView the corresponding index is created.
    *
    * @param id
    *   the view identifier
    * @param project
    *   the view parent project
    * @param rev
    *   the current view revision
    * @param subject
    *   the subject that initiated the action
    */
  def undeprecate(
      id: IdSegment,
      project: ProjectRef,
      rev: Int
  )(implicit subject: Subject): IO[ViewResource] = {
    for {
      (iri, _) <- expandWithContext(fetchContext.onModify, project, id)
      res      <- eval(UndeprecateElasticSearchView(iri, project, rev, subject))
    } yield res
  }.span("undeprecateElasticSearchView")

  /**
    * Deprecates an existing ElasticSearchView without applying preliminary checks on the project status
    *
    * @param id
    *   the view identifier
    * @param project
    *   the view parent project
    * @param rev
    *   the current view revision
    * @param subject
    *   the subject that initiated the action
    */
  private[elasticsearch] def internalDeprecate(id: Iri, project: ProjectRef, rev: Int)(implicit
      subject: Subject
  ): IO[Unit] =
    eval(DeprecateElasticSearchView(id, project, rev, subject)).void

  /**
    * Retrieves a current ElasticSearchView resource.
    *
    * @param id
    *   the identifier that will be expanded to the Iri of the view with its optional rev/tag
    * @param project
    *   the view parent project
    */
  def fetch(id: IdSegmentRef, project: ProjectRef): IO[ViewResource] =
    fetchState(id, project).map { _.toResource(defaultViewDef) }

  def fetchState(
      id: IdSegmentRef,
      project: ProjectRef
  ): IO[ElasticSearchViewState] = {
    for {
      (iri, _) <- expandWithContext(fetchContext.onRead, project, id.value)
      state    <- stateOrNotFound(id, project, iri)
    } yield state
  }.span("fetchElasticSearchView")

  private def stateOrNotFound(id: IdSegmentRef, project: ProjectRef, iri: Iri) = {
    val notFound = ViewNotFound(iri, project)
    id match {
      case Latest(_)        => log.stateOr(project, iri, notFound)
      case Revision(_, rev) => log.stateOr(project, iri, rev, notFound, RevisionNotFound)
      case t: Tag           => IO.raiseError(FetchByTagNotSupported(t))
    }
  }

  /**
    * Retrieves a current IndexingElasticSearchView resource.
    *
    * @param id
    *   the view identifier
    * @param project
    *   the view parent project
    */
  def fetchIndexingView(
      id: IdSegmentRef,
      project: ProjectRef
  ): IO[ActiveViewDef] =
    fetchState(id, project)
      .flatMap { state =>
        IndexingViewDef(state, defaultViewDef, prefix) match {
          case Some(viewDef) =>
            viewDef match {
              case v: ActiveViewDef     => IO.pure(v)
              case v: DeprecatedViewDef => IO.raiseError(ViewIsDeprecated(v.ref.viewId))
            }
          case None          =>
            IO.raiseError(DifferentElasticSearchViewType(state.id.toString, AggregateElasticSearch, ElasticSearch))
        }
      }

  /**
    * Return the existing indexing views in a project in a finite stream
    */
  def currentIndexingViews(project: ProjectRef): SuccessElemStream[IndexingViewDef] =
    log
      .currentStates(Scope.Project(project))
      .evalMapFilter { elem =>
        IO.pure(toIndexViewDef(elem))
      }

  /**
    * Return all existing indexing views in a finite stream
    */
  def currentIndexingViews: SuccessElemStream[IndexingViewDef] =
    log
      .currentStates(Scope.Root)
      .evalMapFilter { elem =>
        IO.pure(toIndexViewDef(elem))
      }

  /**
    * Return the indexing views in a non-ending stream
    */
  def indexingViews(start: Offset): SuccessElemStream[IndexingViewDef] =
    log
      .states(Scope.Root, start)
      .evalMapFilter { elem =>
        IO.pure(toIndexViewDef(elem))
      }

  private def toIndexViewDef(elem: Elem.SuccessElem[ElasticSearchViewState]) =
    elem.traverse { v =>
      IndexingViewDef(v, defaultViewDef, prefix)
    }

  def list(project: ProjectRef): IO[SearchResults[ViewResource]] =
    SearchResults(
      log.currentStates(Scope.Project(project), _.toResource(defaultViewDef))
    ).span("listElasticsearchViews")

  private def eval(cmd: ElasticSearchViewCommand): IO[ViewResource] =
    log
      .evaluate(cmd.project, cmd.id, cmd)
      .map(_._2.toResource(defaultViewDef))

  private def expandWithContext(
      fetchCtx: ProjectRef => IO[ProjectContext],
      ref: ProjectRef,
      id: IdSegment
  ): IO[(Iri, ProjectContext)] =
    fetchCtx(ref).flatMap(pc => expandIri(id, pc).map(_ -> pc))
}

object ElasticSearchViews {

  final val entityType: EntityType = EntityType("elasticsearch")

  private type ElasticsearchLog = ScopedEventLog[
    Iri,
    ElasticSearchViewState,
    ElasticSearchViewCommand,
    ElasticSearchViewEvent,
    ElasticSearchViewRejection
  ]

  /**
    * Iri expansion logic for ElasticSearchViews.
    */
  val expandIri: ExpandIri[InvalidElasticSearchViewId] = new ExpandIri(InvalidElasticSearchViewId.apply)

  /**
    * The default Elasticsearch API mappings
    */
  val mappings: ApiMappings = ApiMappings("view" -> schema.original)

  def projectionName(viewDef: ActiveViewDef): String =
    projectionName(viewDef.ref.project, viewDef.ref.viewId, viewDef.indexingRev)

  def projectionName(state: ElasticSearchViewState): String =
    projectionName(state.project, state.id, state.indexingRev)

  def projectionName(project: ProjectRef, id: Iri, indexingRev: IndexingRev): String = {
    s"elasticsearch-$project-$id-${indexingRev.value}"
  }

  def index(uuid: UUID, indexingRev: IndexingRev, prefix: String): IndexLabel =
    IndexLabel.fromView(prefix, uuid, indexingRev)

  def apply(
      fetchContext: FetchContext,
      contextResolution: ResolverContextResolution,
      validate: ValidateElasticSearchView,
      eventLogConfig: EventLogConfig,
      prefix: String,
      xas: Transactors,
      defaultViewDef: DefaultIndexDef,
      clock: Clock[IO]
  )(implicit uuidF: UUIDF): IO[ElasticSearchViews] =
    ElasticSearchViewJsonLdSourceDecoder(uuidF, contextResolution).map(decoder =>
      new ElasticSearchViews(
        ScopedEventLog(
          definition(validate, clock),
          eventLogConfig,
          xas
        ),
        fetchContext,
        decoder,
        defaultViewDef,
        prefix
      )
    )

  private[elasticsearch] def next(
      state: Option[ElasticSearchViewState],
      event: ElasticSearchViewEvent
  ): Option[ElasticSearchViewState] = {
    // format: off
    def created(e: ElasticSearchViewCreated): Option[ElasticSearchViewState] =
      Option.when(state.isEmpty) {
        ElasticSearchViewState(e.id, e.project, e.uuid, e.value, e.source, Tags.empty, e.rev, IndexingRev.init, deprecated = false,  e.instant, e.subject, e.instant, e.subject)
      }
      
    def updated(e: ElasticSearchViewUpdated): Option[ElasticSearchViewState] = state.map { s =>
      val newIndexingRev = nextIndexingRev(s.value, e.value, s.indexingRev, e.rev)
      s.copy(rev = e.rev, indexingRev = newIndexingRev, value = e.value, source = e.source, updatedAt = e.instant, updatedBy = e.subject)
    }
    // format: on

    def tagAdded(e: ElasticSearchViewTagAdded): Option[ElasticSearchViewState] = state.map { s =>
      s.copy(rev = e.rev, tags = s.tags + (e.tag -> e.targetRev), updatedAt = e.instant, updatedBy = e.subject)
    }

    def deprecated(e: ElasticSearchViewDeprecated): Option[ElasticSearchViewState] = state.map { s =>
      s.copy(rev = e.rev, deprecated = true, updatedAt = e.instant, updatedBy = e.subject)
    }

    def undeprecated(e: ElasticSearchViewUndeprecated): Option[ElasticSearchViewState] = state.map { s =>
      s.copy(rev = e.rev, deprecated = false, updatedAt = e.instant, updatedBy = e.subject)
    }

    event match {
      case e: ElasticSearchViewCreated      => created(e)
      case e: ElasticSearchViewUpdated      => updated(e)
      case e: ElasticSearchViewTagAdded     => tagAdded(e)
      case e: ElasticSearchViewDeprecated   => deprecated(e)
      case e: ElasticSearchViewUndeprecated => undeprecated(e)
    }
  }

  private[elasticsearch] def evaluate(
      validate: ValidateElasticSearchView,
      clock: Clock[IO]
  )(state: Option[ElasticSearchViewState], cmd: ElasticSearchViewCommand)(implicit
      uuidF: UUIDF
  ): IO[ElasticSearchViewEvent] = {

    def create(c: CreateElasticSearchView) = state match {
      case None    =>
        for {
          t <- clock.realTimeInstant
          u <- uuidF()
          _ <- validate(u, IndexingRev.init, c.value)
        } yield ElasticSearchViewCreated(c.id, c.project, u, c.value, c.source, 1, t, c.subject)
      case Some(_) => IO.raiseError(ResourceAlreadyExists(c.id, c.project))
    }

    def update(c: UpdateElasticSearchView) = state match {
      case None                                  => IO.raiseError(ViewNotFound(c.id, c.project))
      case Some(s) if s.rev != c.rev             => IO.raiseError(IncorrectRev(c.rev, s.rev))
      case Some(s) if s.deprecated               => IO.raiseError(ViewIsDeprecated(c.id))
      case Some(s) if c.value.tpe != s.value.tpe =>
        IO.raiseError(DifferentElasticSearchViewType(s.id.toString, c.value.tpe, s.value.tpe))
      case Some(s)                               =>
        val newIndexingRev = nextIndexingRev(s.value, c.value, s.indexingRev, c.rev)
        for {
          _ <- validate(s.uuid, newIndexingRev, c.value)
          t <- clock.realTimeInstant
        } yield ElasticSearchViewUpdated(c.id, c.project, s.uuid, c.value, c.source, s.rev + 1, t, c.subject)
    }

    def deprecate(c: DeprecateElasticSearchView) = state match {
      case None                      => IO.raiseError(ViewNotFound(c.id, c.project))
      case Some(s) if s.rev != c.rev => IO.raiseError(IncorrectRev(c.rev, s.rev))
      case Some(s) if s.deprecated   => IO.raiseError(ViewIsDeprecated(c.id))
      case Some(s)                   =>
        clock.realTimeInstant.map(
          ElasticSearchViewDeprecated(c.id, c.project, s.value.tpe, s.uuid, s.rev + 1, _, c.subject)
        )
    }

    def undeprecate(c: UndeprecateElasticSearchView) = state match {
      case None                      => IO.raiseError(ViewNotFound(c.id, c.project))
      case Some(s) if s.rev != c.rev => IO.raiseError(IncorrectRev(c.rev, s.rev))
      case Some(s) if !s.deprecated  => IO.raiseError(ViewIsNotDeprecated(c.id))
      case Some(s)                   =>
        clock.realTimeInstant.map(
          ElasticSearchViewUndeprecated(c.id, c.project, s.value.tpe, s.uuid, s.rev + 1, _, c.subject)
        )
    }

    cmd match {
      case c: CreateElasticSearchView      => create(c)
      case c: UpdateElasticSearchView      => update(c)
      case c: DeprecateElasticSearchView   => deprecate(c)
      case c: UndeprecateElasticSearchView => undeprecate(c)
    }
  }

  def definition(
      validate: ValidateElasticSearchView,
      clock: Clock[IO]
  )(implicit
      uuidF: UUIDF
  ): ScopedEntityDefinition[
    Iri,
    ElasticSearchViewState,
    ElasticSearchViewCommand,
    ElasticSearchViewEvent,
    ElasticSearchViewRejection
  ] =
    ScopedEntityDefinition.untagged(
      entityType,
      StateMachine(
        None,
        evaluate(validate, clock)(_, _),
        next
      ),
      ElasticSearchViewEvent.serializer,
      ElasticSearchViewState.serializer,
      { s =>
        s.value match {
          case a: AggregateElasticSearchViewValue =>
            Some(a.views.map { v => DependsOn(v.project, v.viewId) }.toSortedSet)
          case _: IndexingElasticSearchViewValue  => None
        }
      },
      onUniqueViolation = (id: Iri, c: ElasticSearchViewCommand) =>
        c match {
          case c: CreateElasticSearchView => ResourceAlreadyExists(id, c.project)
          case c                          => IncorrectRev(c.rev, c.rev + 1)
        }
    )
}
