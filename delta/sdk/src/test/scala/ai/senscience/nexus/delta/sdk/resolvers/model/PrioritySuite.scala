package ai.senscience.nexus.delta.sdk.resolvers.model

import ai.senscience.nexus.delta.sdk.error.FormatErrors.ResolverPriorityIntervalError
import ai.senscience.nexus.testkit.mu.NexusSuite

class PrioritySuite extends NexusSuite {

  test("A priority should be constructed from a valid value") {
    Priority(42).assertRight(Priority.unsafe(42))
  }

  test("A priority should fail for out-of-bounds values") {
    List(-15, -1, 1001, 1000000).foreach { value =>
      Priority(value).assertLeft(ResolverPriorityIntervalError(value, 0, 1000))
    }
  }

}
