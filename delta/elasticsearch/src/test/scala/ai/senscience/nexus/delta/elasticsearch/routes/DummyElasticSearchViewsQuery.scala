package ai.senscience.nexus.delta.elasticsearch.routes

import ai.senscience.nexus.delta.elasticsearch.client.PointInTime
import ai.senscience.nexus.delta.elasticsearch.model.ElasticSearchViewRejection.ViewIsDeprecated
import ai.senscience.nexus.delta.elasticsearch.{ElasticSearchViews, ElasticSearchViewsQuery}
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.model.IdSegment
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.testkit.CirceLiteral.*
import cats.effect.IO
import io.circe.syntax.*
import io.circe.{Json, JsonObject}
import org.http4s.Query

import scala.concurrent.duration.FiniteDuration

private[routes] class DummyElasticSearchViewsQuery(views: ElasticSearchViews) extends ElasticSearchViewsQuery {

  private def toJsonObject(value: Map[String, String]) =
    JsonObject.fromMap(value.map { case (k, v) => k := v })

  override def query(
      id: IdSegment,
      project: ProjectRef,
      query: JsonObject,
      qp: Query
  )(implicit caller: Caller): IO[Json] = {
    for {
      view <- views.fetch(id, project)
      _    <- IO.raiseWhen(view.deprecated)(ViewIsDeprecated(view.id))
    } yield json"""{"id": "$id", "project": "$project"}"""
      .deepMerge(toJsonObject(qp.params).asJson.deepMerge(query.asJson))
  }

  override def createPointInTime(id: IdSegment, project: ProjectRef, keepAlive: FiniteDuration)(implicit
      caller: Caller
  ): IO[PointInTime] =
    IO.pure(PointInTime("xxx"))

  override def deletePointInTime(pointInTime: PointInTime)(implicit caller: Caller): IO[Unit] =
    IO.unit
}
