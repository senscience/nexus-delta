package ai.senscience.nexus.delta.sourcing.stream.utils

import ai.senscience.nexus.testkit.file.TempDirectory
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.effect.IO
import fs2.Stream
import fs2.io.file.Files
import munit.AnyFixture

class StreamingUtilsSuite extends NexusSuite with TempDirectory.Fixture {

  override def munitFixtures: Seq[AnyFixture[?]] = List(tempDirectory)

  private lazy val exportDirectory = tempDirectory()

  private val limitPerFile = 3
  private val lines        = Stream.emits(List("A", "B", "C", "D", "E"))

  test(s"Write stream of lines in a file rotating every $limitPerFile lines") {
    for {
      refCompute <- IO.ref(0)
      computePath = refCompute
                      .updateAndGet(_ + 1)
                      .map { counter => exportDirectory / s"part-$counter.txt" }
      _          <- lines.through(StreamingUtils.writeRotate(computePath, limitPerFile)).compile.drain
      _          <- Files[IO].list(exportDirectory).assertSize(2)
      firstFile   = exportDirectory / "part-1.txt"
      _          <- Files[IO].readUtf8Lines(firstFile).assert("A", "B", "C")
      secondFile  = exportDirectory / "part-2.txt"
      _          <- Files[IO].readUtf8Lines(secondFile).assert("D", "E")
    } yield ()
  }

  test("Create a ndjson stream") {
    val values = Stream.emits(List(json"""{"a" :  1}""", json"""{"b" :  2}""", json"""{"c" :  3}"""))
    values.through(StreamingUtils.ndjson).assert("""{"a":1}""", "\n", """{"b":2}""", "\n", """{"c":3}""", "\n")
  }

}
