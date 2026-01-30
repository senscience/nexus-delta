package ai.senscience.nexus.delta.sdk.model.search

import ai.senscience.nexus.delta.sdk.model.search.ResultEntry.{ScoredResultEntry, UnscoredResultEntry}
import ai.senscience.nexus.delta.sdk.model.search.SearchResults.{ScoredSearchResults, UnscoredSearchResults}
import ai.senscience.nexus.testkit.scalatest.BaseSpec
import cats.syntax.functor.*

class SearchResultsSpec extends BaseSpec {

  "Scored search results" should {
    val entries       = List(ScoredResultEntry(1f, 1), ScoredResultEntry(2f, 2))
    val searchResults = ScoredSearchResults(entries.length.toLong, 2f, entries)

    "map over its value" in {
      val expectedEntries       = List(ScoredResultEntry(1f, 2), ScoredResultEntry(2f, 3))
      val expectedSearchResults = ScoredSearchResults(entries.length.toLong, 2f, expectedEntries)
      searchResults.map(_ + 1) shouldEqual expectedSearchResults
      (searchResults: SearchResults[Int]).map(_ + 1) shouldEqual expectedSearchResults
    }

    "replace its values" in {
      val expectedEntries       = List(ScoredResultEntry(10f, "2"))
      val expectedSearchResults = ScoredSearchResults(expectedEntries.length.toLong, 2f, expectedEntries)
      searchResults.copyWith(expectedEntries) shouldEqual expectedSearchResults
    }
  }

  "Unscored search results" should {
    val entries       = List(UnscoredResultEntry(1), UnscoredResultEntry(2))
    val searchResults = UnscoredSearchResults(entries.length.toLong, entries)

    "map over its value" in {
      val expectedEntries       = List(UnscoredResultEntry(2), UnscoredResultEntry(3))
      val expectedSearchResults = UnscoredSearchResults(entries.length.toLong, expectedEntries)
      searchResults.map(_ + 1) shouldEqual expectedSearchResults
      (searchResults: SearchResults[Int]).map(_ + 1) shouldEqual expectedSearchResults
    }

    "replace its values" in {
      val expectedEntries       = List(UnscoredResultEntry("2"))
      val expectedSearchResults = UnscoredSearchResults(expectedEntries.length.toLong, expectedEntries)
      searchResults.copyWith(expectedEntries) shouldEqual expectedSearchResults
    }
  }

  extension [A](searchResults: SearchResults[A]) {
    def copyWith[B](res: Seq[ResultEntry[B]]): SearchResults[B] = searchResults match {
      case ScoredSearchResults(_, maxScore, _, _) => ScoredSearchResults[B](res.length.toLong, maxScore, res)
      case UnscoredSearchResults(_, _, _)         => UnscoredSearchResults[B](res.length.toLong, res)
    }
  }

}
