package ai.senscience.nexus.delta.elasticsearch.routes

import ai.senscience.nexus.delta.elasticsearch.client.{ElasticSearchRequest, PointInTime}
import ai.senscience.nexus.delta.elasticsearch.model.ElasticSearchViewRejection.ViewIsDeprecated
import ai.senscience.nexus.delta.elasticsearch.{ElasticSearchViews, ElasticSearchViewsQuery}
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.model.IdSegment
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.testkit.CirceLiteral.*
import cats.effect.IO
import io.circe.syntax.*
import io.circe.{Json, JsonObject}

import scala.concurrent.duration.FiniteDuration

private[routes] class DummyElasticSearchViewsQuery(views: ElasticSearchViews) extends ElasticSearchViewsQuery {

  private def toJsonObject(value: Map[String, String]) =
    JsonObject.fromMap(value.map { case (k, v) => k := v })

  override def query(
      id: IdSegment,
      project: ProjectRef,
      request: ElasticSearchRequest
  )(using Caller): IO[Json] = {
    for {
      view <- views.fetch(id, project)
      _    <- IO.raiseWhen(view.deprecated)(ViewIsDeprecated(view.id))
    } yield json"""{"id": "$id", "project": "$project"}"""
      .deepMerge(toJsonObject(request.queryParams).asJson.deepMerge(request.body.asJson))
  }

  override def createPointInTime(id: IdSegment, project: ProjectRef, keepAlive: FiniteDuration)(using
      Caller
  ): IO[PointInTime] =
    IO.pure(PointInTime("xxx"))

  override def deletePointInTime(pointInTime: PointInTime)(using Caller): IO[Unit] =
    IO.unit
}
