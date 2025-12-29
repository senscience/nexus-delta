package ai.senscience.nexus.delta.elasticsearch.query

import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import io.circe.literal.*
import io.circe.{Json, JsonObject}

object SearchByProject {

  def apply(project: ProjectRef): JsonObject =
    JsonObject("query" -> json"""{"term": {"_project": $project} }""")

}
