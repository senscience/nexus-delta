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

  // Builds a resumer over String "views" recording the resumed ones into a Ref.
  private def setup =
    for {
      resumed     <- Ref.of[IO, List[String]](List.empty)
      activations <- ProjectionActivations()
      resumer      = ProjectionResumer[String](
                       module,
                       p => Stream.iterable(active.getOrElse(p, List.empty)),
                       (p, i) => IO.pure(byId.get((p, i))),
                       activations
                     )
    } yield (resumer, activations, resumed)

  // Runs the resumer in the background, gives it a moment to subscribe, publishes, then asserts.
  private def publishing(activation: ProjectionActivation, expected: Set[String]) =
    setup.flatMap { case (resumer, activations, resumed) =>
      resumer.run(v => resumed.update(_ :+ v)).compile.drain.background.surround {
        IO.sleep(200.millis) >>
          activations.publish(activation) >>
          resumed.get.map(_.toSet).assertEquals(expected).eventually
      }
    }

  test("ForProject resumes all the active views of the project") {
    publishing(ProjectionActivation.ForProject(project), Set("a", "b"))
  }

  test("ForProjection resumes the targeted view regardless of project activity") {
    publishing(ProjectionActivation.ForProjection(ProjectionMetadata(module, "a", project, id)), Set("a"))
  }

  test("ForProjection of another module is ignored") {
    publishing(ProjectionActivation.ForProjection(ProjectionMetadata("other", "a", project, id)), Set.empty)
  }

}
