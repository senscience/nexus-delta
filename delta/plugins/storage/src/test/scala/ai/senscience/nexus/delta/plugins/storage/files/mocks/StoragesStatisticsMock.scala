package ai.senscience.nexus.delta.plugins.storage.files.mocks

import ai.senscience.nexus.delta.plugins.storage.storages.StoragesStatistics
import ai.senscience.nexus.delta.plugins.storage.storages.model.StorageStatEntry
import ai.senscience.nexus.delta.sdk.model.IdSegment
import cats.effect.IO
import ch.epfl.bluebrain.nexus.delta.sourcing.model.ProjectRef

object StoragesStatisticsMock {

  def unimplemented: StoragesStatistics = withMockedGet((_, _) => IO(???))

  def withMockedGet(getMock: (IdSegment, ProjectRef) => IO[StorageStatEntry]): StoragesStatistics =
    (idSegment: IdSegment, project: ProjectRef) => getMock(idSegment, project)
}
