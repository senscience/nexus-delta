package ai.senscience.nexus.delta.sdk.identities.model

import ai.senscience.nexus.delta.sourcing.model.Identity
import ai.senscience.nexus.testkit.mu.NexusSuite

class CallerSuite extends NexusSuite {

  test("A Caller should append the subject to the identities set") {
    val caller = Caller(Identity.Anonymous, Set.empty)
    assertEquals(caller.subject, Identity.Anonymous)
    assertEquals(caller.identities, Set[Identity](Identity.Anonymous))
  }

}
