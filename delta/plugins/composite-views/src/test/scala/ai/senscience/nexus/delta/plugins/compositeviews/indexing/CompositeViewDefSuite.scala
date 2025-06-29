package ai.senscience.nexus.delta.plugins.compositeviews.indexing

import ai.senscience.nexus.delta.plugins.compositeviews.CompositeViewsFixture
import ai.senscience.nexus.delta.plugins.compositeviews.model.CompositeView.{Interval, RebuildStrategy}
import ai.senscience.nexus.delta.plugins.compositeviews.model.CompositeViewSource
import ai.senscience.nexus.delta.plugins.compositeviews.stream.CompositeGraphStream
import ai.senscience.nexus.delta.rdf.graph.NTriples
import ai.senscience.nexus.delta.sdk.views.ViewRef
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.state.GraphResource
import ai.senscience.nexus.delta.sourcing.stream.pipes.FilterDeprecated
import ai.senscience.nexus.delta.sourcing.stream.{ElemStream, NoopSink, RemainingElems, Source}
import ai.senscience.nexus.testkit.mu.NexusSuite
import ai.senscience.nexus.testkit.mu.ce.PatienceConfig
import cats.effect.{IO, Ref}
import fs2.Stream
import shapeless.Typeable

import scala.concurrent.duration.*

class CompositeViewDefSuite extends NexusSuite with CompositeViewsFixture {

  implicit private val patienceConfig: PatienceConfig = PatienceConfig(1.second, 50.millis)

  private val sleep = IO.sleep(50.millis)

  test("Compile correctly the source") {

    def makeSource(nameValue: String): Source = new Source {
      override type Out = Unit
      override def outType: Typeable[Unit]                 = Typeable[Unit]
      override def apply(offset: Offset): ElemStream[Unit] = Stream.empty[IO]
      override def name: String                            = nameValue
    }

    val graphStream = new CompositeGraphStream {
      override def main(source: CompositeViewSource, project: ProjectRef): Source                                    = makeSource("main")
      override def rebuild(source: CompositeViewSource, project: ProjectRef): Source                                 = makeSource("rebuild")
      override def remaining(source: CompositeViewSource, project: ProjectRef): Offset => IO[Option[RemainingElems]] =
        _ => IO.none
    }

    CompositeViewDef
      .compileSource(
        project.ref,
        _ => Right(FilterDeprecated.withConfig(())),
        graphStream,
        new NoopSink[NTriples]()
      )(projectSource)
      .map { case (id, mainSource, rebuildSource, operation) =>
        assertEquals(id, projectSource.id)
        assertEquals(mainSource.name, "main")
        assertEquals(rebuildSource.name, "rebuild")
        assertEquals(operation.outType.describe, Typeable[GraphResource].describe)
      }
  }

  test("Compile correctly an Elasticsearch projection") {
    CompositeViewDef
      .compileTarget(
        _ => Right(FilterDeprecated.withConfig(())),
        _ => new NoopSink[GraphResource]()
      )(esProjection)
      .map(_._1)
      .assertEquals(esProjection.id)
  }

  test("Compile correctly a Sparql projection") {
    CompositeViewDef
      .compileTarget(
        _ => Right(FilterDeprecated.withConfig(())),
        _ => new NoopSink[GraphResource]()
      )(blazegraphProjection)
      .map(_._1)
      .assertEquals(blazegraphProjection.id)
  }

  private def rebuild(strategy: Option[RebuildStrategy]) = {
    for {
      start <- Ref.of[IO, Boolean](false)
      value <- Ref.of[IO, Int](0)
      reset <- Ref.of[IO, Int](0)
      inc    = Stream.eval(value.getAndUpdate(_ + 1))
      result = CompositeViewDef
                 .rebuild[Int](ViewRef(projectRef, id), strategy, start.get, reset.update(_ + 1))
      _     <- result(inc).compile.drain.start
    } yield (start, value, reset)
  }

  test("Not execute the stream if no rebuild strategy is defined") {
    for {
      (start, value, reset) <- rebuild(None)
      _                     <- start.set(true)
      _                     <- sleep
      _                     <- value.get.assertEquals(0)
      _                     <- reset.get.assertEquals(0)
    } yield ()
  }

  test("Execute the stream if a rebuild strategy is defined") {
    for {
      (start, value, reset) <- rebuild(Some(Interval(50.millis)))
      _                     <- start.set(true)
      _                     <- value.get.assertEquals(4).eventually
      _                     <- reset.get.assertEquals(4).eventually
      // This should stop the stream
      _                     <- start.set(false)
      _                     <- sleep
      paused                <- value.get
      _                     <- sleep
      _                     <- value.get.assertEquals(paused).eventually
      // We resume the stream
      _                     <- start.set(true)
      _                     <- value.get.assertEquals(paused + 4).eventually
    } yield ()
  }

}
