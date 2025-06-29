package ai.senscience.nexus.delta.plugins.blazegraph

import ai.senscience.nexus.delta.plugins.blazegraph.model.{BlazegraphViewState, BlazegraphViewValue, ViewResource}
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sourcing.model.Identity.{Anonymous, Subject}
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import io.circe.Json

import java.time.Instant
import java.util.UUID

object BlazegraphViewsGen {

  def resourceFor(
      id: Iri,
      project: ProjectRef,
      value: BlazegraphViewValue,
      uuid: UUID = UUID.randomUUID(),
      source: Json = Json.obj(),
      rev: Int = 1,
      indexingRev: Int = 1,
      deprecated: Boolean = false,
      createdBy: Subject = Anonymous,
      updatedBy: Subject = Anonymous
  ): ViewResource =
    BlazegraphViewState(
      id,
      project,
      uuid,
      value,
      source,
      rev,
      indexingRev,
      deprecated,
      Instant.EPOCH,
      createdBy,
      Instant.EPOCH,
      updatedBy
    ).toResource
}
