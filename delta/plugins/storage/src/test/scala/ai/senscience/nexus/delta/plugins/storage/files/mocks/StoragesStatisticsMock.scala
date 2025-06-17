package ai.senscience.nexus.delta.plugins.storage.files.mocks

import ai.senscience.nexus.delta.plugins.storage.storages.StoragesStatistics
import ai.senscience.nexus.delta.plugins.storage.storages.model.StorageStatEntry
import ai.senscience.nexus.delta.sdk.model.IdSegment
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import cats.effect.IO

object StoragesStatisticsMock {

  def unimplemented: StoragesStatistics = withMockedGet((_, _) => IO(???))

  def withMockedGet(getMock: (IdSegment, ProjectRef) => IO[StorageStatEntry]): StoragesStatistics =
    (idSegment: IdSegment, project: ProjectRef) => getMock(idSegment, project)
}
