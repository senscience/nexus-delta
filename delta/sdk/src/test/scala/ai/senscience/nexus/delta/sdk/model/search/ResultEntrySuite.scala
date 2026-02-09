package ai.senscience.nexus.delta.sdk.model.search

import ai.senscience.nexus.delta.sdk.model.search.ResultEntry.{ScoredResultEntry, UnscoredResultEntry}
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.syntax.functor.*

class ResultEntrySuite extends NexusSuite {

  test("A ScoredResultEntry should map over its value") {
    val expected = ScoredResultEntry(1f, 2)
    val entry    = ScoredResultEntry(1f, 1)
    assertEquals(entry.map(_ + 1), expected)
    assertEquals((entry: ResultEntry[Int]).map(_ + 1), expected)
  }

  test("An UnscoredResultEntry should map over its value") {
    val expected = UnscoredResultEntry(2)
    val entry    = UnscoredResultEntry(1)
    assertEquals(entry.map(_ + 1), expected)
    assertEquals((entry: ResultEntry[Int]).map(_ + 1), expected)
  }
}
