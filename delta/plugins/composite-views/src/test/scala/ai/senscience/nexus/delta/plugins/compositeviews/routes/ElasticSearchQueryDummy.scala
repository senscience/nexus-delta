package ai.senscience.nexus.delta.plugins.compositeviews.routes

import ai.senscience.nexus.delta.elasticsearch.client.ElasticSearchRequest
import ai.senscience.nexus.delta.plugins.compositeviews.model.CompositeViewRejection.ViewIsDeprecated
import ai.senscience.nexus.delta.plugins.compositeviews.{CompositeViews, ElasticSearchQuery}
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.model.IdSegment
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import cats.effect.IO
import io.circe.Json

class ElasticSearchQueryDummy(
    projectionQuery: Map[(IdSegment, ElasticSearchRequest), Json],
    projectionsQuery: Map[ElasticSearchRequest, Json],
    views: CompositeViews
) extends ElasticSearchQuery {

  private def fetchView(id: IdSegment, project: ProjectRef) =
    views.fetch(id, project).flatTap { view =>
      IO.raiseWhen(view.deprecated)(ViewIsDeprecated(view.id))
    }

  override def query(
      id: IdSegment,
      projectionId: IdSegment,
      project: ProjectRef,
      request: ElasticSearchRequest
  )(using Caller): IO[Json] =
    fetchView(id, project)
      .as(projectionQuery(projectionId -> request))

  override def queryProjections(
      id: IdSegment,
      project: ProjectRef,
      request: ElasticSearchRequest
  )(using Caller): IO[Json] =
    fetchView(id, project)
      .as(projectionsQuery(request))
}
