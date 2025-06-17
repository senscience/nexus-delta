package ai.senscience.nexus.delta.plugins.storage.storages

import ai.senscience.nexus.delta.plugins.storage.storages.model.{StorageState, StorageValue}
import ai.senscience.nexus.delta.sourcing.model.Identity.{Anonymous, Subject}
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ch.epfl.bluebrain.nexus.delta.rdf.IriOrBNode.Iri
import io.circe.Json

import java.time.Instant

object StorageGen {

  def storageState(
      id: Iri,
      project: ProjectRef,
      value: StorageValue,
      source: Json = Json.obj(),
      rev: Int = 1,
      deprecated: Boolean = false,
      createdBy: Subject = Anonymous,
      updatedBy: Subject = Anonymous
  ): StorageState = {
    StorageState(
      id,
      project,
      value,
      source,
      rev,
      deprecated,
      Instant.EPOCH,
      createdBy,
      Instant.EPOCH,
      updatedBy
    )
  }

  def resourceFor(
      id: Iri,
      project: ProjectRef,
      value: StorageValue,
      source: Json = Json.obj(),
      rev: Int = 1,
      deprecated: Boolean = false,
      createdBy: Subject = Anonymous,
      updatedBy: Subject = Anonymous
  ): StorageResource =
    storageState(id, project, value, source, rev, deprecated, createdBy, updatedBy).toResource

}
