package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.testkit.mu.NexusSuite
import ai.senscience.nexus.testkit.mu.ce.PatienceConfig
import cats.effect.{IO, Ref}
import fs2.Stream

import scala.concurrent.duration.*

class ProjectionResumerSuite extends NexusSuite {

  private given PatienceConfig = PatienceConfig(2.seconds, 20.millis)

  private val module   = "test"
  private val project  = ProjectRef.unsafe("org", "proj")
  private val inactive = ProjectRef.unsafe("org", "inactive")
  private val id       = nxv + "view1"

  private val active = Map(project -> List("a", "b"), inactive -> List("c"))
  private val byId   = Map((project, id) -> "a", (inactive, id) -> "c")

  // Builds a resumer over String "views" recording the resumed ones into a Ref, with the given active projects.
  private def setup(activeSet: Set[ProjectRef]) =
    for {
      resumed        <- Ref.of[IO, List[String]](List.empty)
      activations    <- ProjectionActivations()
      projectActivity = new ProjectActivity {
                          override def isActive(p: ProjectRef): IO[Boolean] = IO.pure(activeSet.contains(p))
                          override def activations: Stream[IO, ProjectRef]  = Stream.empty
                          override def activeProjects: IO[List[ProjectRef]] = IO.pure(activeSet.toList)
                        }
      resumer         = ProjectionResumer[String](
                          module,
                          p => Stream.iterable(active.getOrElse(p, List.empty)),
                          (p, i) => IO.pure(byId.get((p, i))),
                          projectActivity,
                          activations
                        )
    } yield (resumer, activations, resumed)

  // Runs the resumer in the background, gives it a moment to subscribe, publishes, then asserts.
  private def publishing(activeProjects: Set[ProjectRef])(
      activation: ProjectionActivation,
      expected: Set[String]
  ) =
    setup(activeProjects).flatMap { case (resumer, activations, resumed) =>
      resumer.run(v => resumed.update(_ :+ v)).compile.drain.background.surround {
        IO.sleep(200.millis) >>
          activations.publish(activation) >>
          resumed.get.map(_.toSet).assertEquals(expected).eventually
      }
    }

  test("ForProject resumes all the active views of the project") {
    publishing(Set(project))(ProjectionActivation.ForProject(project), Set("a", "b"))
  }

  test("ForProjection resumes the targeted view regardless of project activity") {
    publishing(Set.empty)(ProjectionActivation.ForProjection(ProjectionMetadata(module, "a", project, id)), Set("a"))
  }

  test("ForProjection of another module is ignored") {
    publishing(Set(project))(
      ProjectionActivation.ForProjection(ProjectionMetadata("other", "a", project, id)),
      Set.empty
    )
  }

  test("isActive reflects the underlying project activity") {
    setup(Set(project)).flatMap { case (resumer, _, _) =>
      resumer.isActive(project).assertEquals(true) >> resumer.isActive(inactive).assertEquals(false)
    }
  }

}
