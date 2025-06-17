package ai.senscience.nexus.delta.sdk.model.search

import ai.senscience.nexus.delta.sdk.model.search.ResultEntry.{ScoredResultEntry, UnscoredResultEntry}
import ai.senscience.nexus.testkit.scalatest.BaseSpec
import cats.syntax.functor.*

class ResultEntrySpec extends BaseSpec {

  "A ScoredResultEntry entry" should {

    "map over its value" in {
      val expected = ScoredResultEntry(1f, 2)
      val entry    = ScoredResultEntry(1f, 1)
      entry.map(_ + 1) shouldEqual expected
      (entry: ResultEntry[Int]).map(_ + 1) shouldEqual expected
    }
  }

  "An UnscoredResultEntry entry" should {

    "map over its value" in {
      val expected = UnscoredResultEntry(2)
      val entry    = UnscoredResultEntry(1)
      entry.map(_ + 1) shouldEqual expected
      (entry: ResultEntry[Int]).map(_ + 1) shouldEqual expected
    }
  }

}
