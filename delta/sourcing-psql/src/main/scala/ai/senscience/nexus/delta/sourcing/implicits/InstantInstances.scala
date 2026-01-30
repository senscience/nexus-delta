package ai.senscience.nexus.delta.sourcing.implicits

import cats.Order

import java.time.Instant

trait InstantInstances {

  given Order[Instant] = (x: Instant, y: Instant) => x.compareTo(y)

}
