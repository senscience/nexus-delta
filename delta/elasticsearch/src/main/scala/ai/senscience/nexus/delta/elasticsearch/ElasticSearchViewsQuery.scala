package ai.senscience.nexus.delta.elasticsearch

import ai.senscience.nexus.delta.elasticsearch.client.{ElasticSearchClient, ElasticSearchRequest, IndexLabel, PointInTime}
import ai.senscience.nexus.delta.elasticsearch.model.ElasticSearchViewRejection.{DifferentElasticSearchViewType, ViewIsDeprecated}
import ai.senscience.nexus.delta.elasticsearch.model.ElasticSearchViewValue.{AggregateElasticSearchViewValue, IndexingElasticSearchViewValue}
import ai.senscience.nexus.delta.elasticsearch.model.{permissions, ElasticSearchViewRejection, ElasticSearchViewState, ElasticSearchViewType}
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress.Project as ProjectAcl
import ai.senscience.nexus.delta.sdk.error.ServiceError.AuthorizationFailed
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.model.IdSegment
import ai.senscience.nexus.delta.sdk.syntax.surround
import ai.senscience.nexus.delta.sdk.views.View.{AggregateView, IndexingView}
import ai.senscience.nexus.delta.sdk.views.{View, ViewRef, ViewsStore}
import ai.senscience.nexus.delta.sourcing.Transactors
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import cats.effect.IO
import cats.syntax.all.*
import io.circe.Json
import org.typelevel.otel4s.trace.Tracer

import scala.concurrent.duration.FiniteDuration

/**
  * Allows operations on Elasticsearch views
  */
trait ElasticSearchViewsQuery {

  /**
    * Queries the elasticsearch index (or indices) managed by the view with the passed ''id''. We check for the caller
    * to have the necessary query permissions on the view before performing the query.
    *
    * @param id
    *   the id of the view either in Iri or aliased form
    * @param project
    *   the project where the view exists
    * @param request
    *   the elasticsearch request to run
    */
  def query(
      id: IdSegment,
      project: ProjectRef,
      request: ElasticSearchRequest
  )(using Caller): IO[Json]

  /**
    * Queries the elasticsearch index (or indices) managed by the view. We check for the caller to have the necessary
    * query permissions on the view before performing the query.
    * @param view
    *   the reference to the view
    * @param request
    *   the elasticsearch request to run
    */
  def query(view: ViewRef, request: ElasticSearchRequest)(using Caller): IO[Json] =
    this.query(view.viewId, view.project, request)

  /**
    * Creates a point-in-time to be used in further searches
    *
    * @see
    *   https://www.elastic.co/guide/en/elasticsearch/reference/current/point-in-time-api.html
    * @param id
    *   the target view
    * @param project
    *   project reference in which the view is
    * @param keepAlive
    *   extends the time to live of the corresponding point in time
    */
  def createPointInTime(id: IdSegment, project: ProjectRef, keepAlive: FiniteDuration)(using Caller): IO[PointInTime]

  /**
    * Deletes the given point-in-time
    *
    * @see
    *   https://www.elastic.co/guide/en/elasticsearch/reference/current/point-in-time-api.html
    */
  def deletePointInTime(pointInTime: PointInTime)(using Caller): IO[Unit]

}

/**
  * Operations that interact with the elasticsearch indices managed by ElasticSearchViews.
  */
final class ElasticSearchViewsQueryImpl private[elasticsearch] (
    viewStore: ViewsStore[ElasticSearchViewRejection],
    aclCheck: AclCheck,
    client: ElasticSearchClient
)(using Tracer[IO])
    extends ElasticSearchViewsQuery {

  override def query(
      id: IdSegment,
      project: ProjectRef,
      request: ElasticSearchRequest
  )(using Caller): IO[Json] = {
    for {
      view    <- viewStore.fetch(id, project)
      indices <- extractIndices(view)
      search  <- client.search(request, indices)
    } yield search
  }.surround("elasticsearchUserQuery")

  private def extractIndices(view: View)(using Caller): IO[Set[String]] = view match {
    case v: IndexingView  =>
      aclCheck
        .authorizeForOr(v.ref.project, v.permission)(AuthorizationFailed(v.ref.project, v.permission))
        .as(Set(v.index))
    case v: AggregateView =>
      aclCheck.mapFilter[IndexingView, String](
        v.views,
        v => ProjectAcl(v.ref.project) -> v.permission,
        _.index
      )
  }

  override def createPointInTime(id: IdSegment, project: ProjectRef, keepAlive: FiniteDuration)(using
      Caller
  ): IO[PointInTime] =
    for {
      _     <- aclCheck.authorizeForOr(project, permissions.write)(AuthorizationFailed(project, permissions.write))
      view  <- viewStore.fetch(id, project)
      index <- indexOrError(view, id)
      pit   <- client.createPointInTime(index, keepAlive)
    } yield pit

  override def deletePointInTime(pointInTime: PointInTime)(using Caller): IO[Unit] =
    client.deletePointInTime(pointInTime)

  private def indexOrError(view: View, id: IdSegment): IO[IndexLabel] = view match {
    case IndexingView(_, index, _) => IO.fromEither(IndexLabel(index))
    case _: AggregateView          =>
      IO.raiseError(
        DifferentElasticSearchViewType(
          id.toString,
          ElasticSearchViewType.AggregateElasticSearch,
          ElasticSearchViewType.ElasticSearch
        )
      )
  }

}

object ElasticSearchViewsQuery {

  final def apply(
      aclCheck: AclCheck,
      views: ElasticSearchViews,
      client: ElasticSearchClient,
      prefix: String,
      xas: Transactors
  )(using Tracer[IO]): ElasticSearchViewsQuery =
    new ElasticSearchViewsQueryImpl(
      ViewsStore[ElasticSearchViewRejection, ElasticSearchViewState](
        ElasticSearchViewState.serializer,
        views.fetchState,
        view =>
          IO.raiseWhen(view.deprecated)(ViewIsDeprecated(view.id))
            .as(viewIriOrIndexingView(prefix, view)),
        xas
      ),
      aclCheck,
      client
    )

  private def viewIriOrIndexingView(prefix: String, view: ElasticSearchViewState): Either[Iri, IndexingView] =
    view.value match {
      case _: AggregateElasticSearchViewValue => Left(view.id)
      case i: IndexingElasticSearchViewValue  =>
        IndexingView(
          ViewRef(view.project, view.id),
          ElasticSearchViews.index(view.uuid, view.indexingRev, prefix).value,
          i.permission
        ).asRight
    }
}
