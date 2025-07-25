package ai.senscience.nexus.delta.elasticsearch

import ai.senscience.nexus.delta.elasticsearch.model.{ElasticSearchViewState, ElasticSearchViewValue, ViewResource}
import ai.senscience.nexus.delta.elasticsearch.views.DefaultIndexDef
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sdk.views.IndexingRev
import ai.senscience.nexus.delta.sourcing.model.Identity.{Anonymous, Subject}
import ai.senscience.nexus.delta.sourcing.model.{ProjectRef, Tags}
import io.circe.{Json, JsonObject}

import java.time.Instant
import java.util.UUID

object ElasticSearchViewGen {

  def stateFor(
      id: Iri,
      project: ProjectRef,
      value: ElasticSearchViewValue,
      uuid: UUID = UUID.randomUUID(),
      source: Json = Json.obj(),
      rev: Int = 1,
      indexingRev: IndexingRev = IndexingRev.init,
      deprecated: Boolean = false,
      tags: Tags = Tags.empty,
      createdBy: Subject = Anonymous,
      updatedBy: Subject = Anonymous
  ): ElasticSearchViewState =
    ElasticSearchViewState(
      id,
      project,
      uuid,
      value,
      source,
      tags,
      rev,
      indexingRev,
      deprecated,
      Instant.EPOCH,
      createdBy,
      Instant.EPOCH,
      updatedBy
    )

  def resourceFor(
      id: Iri,
      project: ProjectRef,
      value: ElasticSearchViewValue,
      uuid: UUID = UUID.randomUUID(),
      source: Json = Json.obj(),
      rev: Int = 1,
      indexingRev: IndexingRev = IndexingRev.init,
      deprecated: Boolean = false,
      tags: Tags = Tags.empty,
      createdBy: Subject = Anonymous,
      updatedBy: Subject = Anonymous
  ): ViewResource =
    stateFor(
      id,
      project,
      value,
      uuid,
      source,
      rev,
      indexingRev,
      deprecated,
      tags,
      createdBy,
      updatedBy
    ).toResource(DefaultIndexDef(JsonObject.empty, JsonObject.empty))
}
