package ai.senscience.nexus.testkit.file

import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.effect.IO
import cats.effect.kernel.Resource
import fs2.io.file.{Files, Path}
import munit.catseffect.IOFixture

object TempDirectory {

  trait Fixture { self: NexusSuite =>
    val tempDirectory: IOFixture[Path] =
      ResourceSuiteLocalFixture(
        "tempDirectory",
        Resource.make(Files[IO].createTempDirectory)(Files[IO].deleteRecursively)
      )
  }

}
