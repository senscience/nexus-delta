package ai.senscience.nexus.delta.elasticsearch.query

import ai.senscience.nexus.delta.elasticsearch.client.ElasticSearchRequest
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import io.circe.syntax.{EncoderOps, KeyOps}
import io.circe.{Json, JsonObject}

object FilterByProject {
  def apply(project: ProjectRef, request: ElasticSearchRequest): ElasticSearchRequest = {
    request.mapBody { body =>
      val userQuery = body("query")
      val must      = userQuery.map("must" -> _)
      val filter    = "filter" -> Json.obj("term" -> Json.obj("_project" := project))
      body.add(
        "query",
        Json.obj(
          "bool" ->
            JsonObject
              .fromIterable(
                must ++ Some(filter)
              )
              .asJson
        )
      )
    }
  }

}
