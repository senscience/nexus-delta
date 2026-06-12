package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import cats.effect.IO
import fs2.Stream
import fs2.concurrent.Topic

/**
  * Test fixture: a [[ProjectActivity]] reporting the given projects as active, whose activation stream is backed by a
  * [[Topic]] that the test can publish to via [[publishActivation]].
  */
final class ControllableProjectActivity private (active: Set[ProjectRef], topic: Topic[IO, ProjectRef])
    extends ProjectActivity {

  override def isActive(p: ProjectRef): IO[Boolean] = IO.pure(active.contains(p))
  override def activations: Stream[IO, ProjectRef]  = topic.subscribe(Int.MaxValue)
  override def activeProjects: IO[List[ProjectRef]] = IO.pure(active.toList)

  /** Publishes an activation event for the given project. */
  def publishActivation(project: ProjectRef): IO[Unit] = topic.publish1(project).void
}

object ControllableProjectActivity {

  def apply(active: ProjectRef*): IO[ControllableProjectActivity] =
    Topic[IO, ProjectRef].map(new ControllableProjectActivity(active.toSet, _))
}
