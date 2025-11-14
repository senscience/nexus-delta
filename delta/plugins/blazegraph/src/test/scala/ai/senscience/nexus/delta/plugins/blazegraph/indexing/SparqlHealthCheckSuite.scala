package ai.senscience.nexus.delta.plugins.blazegraph.indexing

import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.effect.IO
import fs2.Stream
import fs2.concurrent.SignallingRef

class SparqlHealthCheckSuite extends NexusSuite {

  test("Health check should work as expected") {
    for {
      failingSignal <- SignallingRef.of[IO, Boolean](true)
      healthStream   = Stream.emits(List(false, true, false, true))
      _             <- SparqlHealthCheck.stream(healthStream, failingSignal).compile.drain
      _             <- failingSignal.get.assertEquals(false)
    } yield ()
  }

}
