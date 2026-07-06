package ai.senscience.nexus.testkit

import cats.effect.{IO, Resource}
import org.testcontainers.containers.GenericContainer

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.jdk.DurationConverters.ScalaDurationOps

object TestContainers {

  /**
    * A started testcontainers container, scoped to a [[Resource]]. The concrete container type is intentionally erased:
    * callers only need the [[GenericContainer]] surface (`getHost`, `getMappedPort`, …).
    */
  type ContainerResource = Resource[IO, GenericContainer[?]]

  /**
    * Wraps a testcontainers [[GenericContainer]] in a [[Resource]]: reuse is disabled and the startup timeout applied,
    * the container is started on acquisition and stopped on release. Start/stop run on the blocking pool.
    */
  def resource(newContainer: => GenericContainer[?], startupTimeout: FiniteDuration = 60.seconds): ContainerResource =
    Resource.make(
      IO.blocking {
        val container = newContainer
        container.withReuse(false)
        container.withStartupTimeout(startupTimeout.toJava)
        container.start()
        container
      }
    )(container => IO.blocking(container.stop()))
}
