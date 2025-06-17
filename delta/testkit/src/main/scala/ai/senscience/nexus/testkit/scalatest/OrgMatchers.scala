package ai.senscience.nexus.testkit.scalatest

import io.circe.Json
import org.scalatest.matchers.BeMatcher

object OrgMatchers {
  def deprecated: BeMatcher[Json] = MatcherBuilders.deprecated("organization")
}
