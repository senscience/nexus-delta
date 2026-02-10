package ai.senscience.nexus.delta.sdk.permissions.model

import ai.senscience.nexus.delta.sdk.generators.PermissionsGen
import ai.senscience.nexus.delta.sourcing.model.Identity
import ai.senscience.nexus.testkit.mu.NexusSuite

import java.time.Instant

class PermissionsStateSuite extends NexusSuite {

  private val minimum    = Set(Permission.unsafe("my/permission"))
  private val additional = Set(Permission.unsafe("my/additional"))

  test("A PermissionsState initial should return its resource representation") {
    assertEquals(PermissionsState.initial(minimum).toResource(minimum), PermissionsGen.resourceFor(minimum, rev = 0))
  }

  test("A PermissionsState current should return its resource representation") {
    val current = PermissionsState(
      3,
      additional,
      Instant.EPOCH,
      Identity.Anonymous,
      Instant.EPOCH,
      Identity.Anonymous
    )
    assertEquals(current.toResource(minimum), PermissionsGen.resourceFor(minimum ++ additional, rev = 3))
  }

}
