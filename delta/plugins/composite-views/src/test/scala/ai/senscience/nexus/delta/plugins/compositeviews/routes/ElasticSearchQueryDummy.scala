package ai.senscience.nexus.delta.plugins.compositeviews.routes

import ai.senscience.nexus.delta.plugins.compositeviews.model.CompositeViewRejection.ViewIsDeprecated
import ai.senscience.nexus.delta.plugins.compositeviews.{CompositeViews, ElasticSearchQuery}
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.model.IdSegment
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import cats.effect.IO
import io.circe.{Json, JsonObject}
import org.http4s.Query

class ElasticSearchQueryDummy(
    projectionQuery: Map[(IdSegment, JsonObject), Json],
    projectionsQuery: Map[JsonObject, Json],
    views: CompositeViews
) extends ElasticSearchQuery {

  override def query(
      id: IdSegment,
      projectionId: IdSegment,
      project: ProjectRef,
      query: JsonObject,
      qp: Query
  )(implicit caller: Caller): IO[Json] =
    for {
      view <- views.fetch(id, project)
      _    <- IO.raiseWhen(view.deprecated)(ViewIsDeprecated(view.id))
    } yield projectionQuery(projectionId -> query)

  override def queryProjections(
      id: IdSegment,
      project: ProjectRef,
      query: JsonObject,
      qp: Query
  )(implicit caller: Caller): IO[Json] =
    for {
      view <- views.fetch(id, project)
      _    <- IO.raiseWhen(view.deprecated)(ViewIsDeprecated(view.id))
    } yield projectionsQuery(query)

}
