package ai.senscience.nexus.delta.rdf.utils

import ai.senscience.nexus.delta.rdf.syntax.*
import ai.senscience.nexus.testkit.mu.NexusSuite

import java.nio.file.Paths

class PathUtilsSuite extends NexusSuite {
  private val `/tmp/a`     = Paths.get("/tmp/a")
  private val `/tmp/a/b/c` = Paths.get("/tmp/a/b/c")
  private val `/tmp`       = Paths.get("/tmp")

  test("A Path is descendant of another path") {
    assert(`/tmp/a/b/c`.descendantOf(`/tmp/a`))
    assert(`/tmp/a`.descendantOf(`/tmp`))
  }

  test("A Path is NOT descendant of another path") {
    assert(! `/tmp`.descendantOf(`/tmp/a`))
    assert(! `/tmp/a`.descendantOf(`/tmp/a`))
  }
}
