package ai.senscience.nexus.tests

import ai.senscience.nexus.delta.kernel.utils.ClasspathResourceLoader
import ai.senscience.nexus.tests.Optics.filterNestedKeys
import io.circe.Json
import org.scalactic.source.Position
import ai.senscience.nexus.testkit.scalatest.ce.CatsIOValues
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers

object StatisticsAssertions extends Matchers with CatsIOValues {

  private val loader = ClasspathResourceLoader()

  def expectStats(json: Json)(total: Int, processed: Int, evaluated: Int, discarded: Int, failed: Int, remaining: Int)(
      using Position
  ): Assertion = {
    val expected = loader
      .jsonContentOf(
        "kg/views/statistics.json",
        "total"      -> total,
        "processed"  -> processed,
        "evaluated"  -> evaluated,
        "discarded"  -> discarded,
        "failed"     -> failed,
        "remaining"  -> remaining,
        "has_events" -> (total != 0)
      )
      .accepted
    filterNestedKeys("lastEventDateTime", "lastProcessedEventDateTime")(json) shouldEqual expected
  }

  def expectEmptyStats(json: Json)(using Position): Assertion =
    expectStats(json)(0, 0, 0, 0, 0, 0)

}
