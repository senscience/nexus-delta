package ai.senscience.nexus.delta.elasticsearch.routes

import ai.senscience.nexus.delta.kernel.search.Pagination.FromPagination
import DummyMainIndexQuery.{aggregationResponse, allowedPage, listResponse}
import ai.senscience.nexus.delta.elasticsearch.query.{MainIndexQuery, MainIndexRequest}
import ai.senscience.nexus.delta.sdk.error.ServiceError.AuthorizationFailed
import ai.senscience.nexus.delta.sdk.model.search.{AggregationResult, SearchResults}
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.testkit.CirceLiteral.circeLiteralSyntax
import cats.effect.IO
import io.circe.{Json, JsonObject}
import org.http4s.Query

class DummyMainIndexQuery extends MainIndexQuery {

  override def search(project: ProjectRef, query: JsonObject, qp: Query): IO[Json] =
    IO.raiseError(AuthorizationFailed("Fail !!!!"))

  override def list(request: MainIndexRequest, projects: Set[ProjectRef]): IO[SearchResults[JsonObject]] =
    if (request.pagination == allowedPage)
      IO.pure(SearchResults(1, List(listResponse)))
    else
      IO.raiseError(AuthorizationFailed("Fail !!!!"))

  override def aggregate(request: MainIndexRequest, projects: Set[ProjectRef]): IO[AggregationResult] =
    IO.pure(AggregationResult(1, aggregationResponse))
}

object DummyMainIndexQuery {

  private val allowedPage             = FromPagination(0, 5)
  val listResponse: JsonObject        = jobj"""{"http://localhost/projects": "all"}"""
  val aggregationResponse: JsonObject = jobj"""{"types": "something"}"""
}
