package ai.senscience.nexus.testkit.scalatest

import io.circe.{Decoder, Json}
import org.scalatest.matchers.{HavePropertyMatchResult, HavePropertyMatcher}

import scala.reflect.ClassTag

object JsonMatchers {
  def field[A: {Decoder, ClassTag}](key: String, expectedValue: A)(using ev: Null <:< A): HavePropertyMatcher[Json, A] =
    HavePropertyMatcher { json =>
      val actual = json.hcursor.downField(key).as[A].toOption
      HavePropertyMatchResult(
        actual.contains(expectedValue),
        key,
        expectedValue,
        actual.orNull
      )
    }

  def fieldThatEndsWith(key: String, expectedEnding: String): HavePropertyMatcher[Json, String] = HavePropertyMatcher {
    json =>
      val actual = json.hcursor.downField(key).as[String].toOption
      HavePropertyMatchResult(
        actual.exists(_.endsWith(expectedEnding)),
        key,
        "ends with " + expectedEnding,
        actual.orNull
      )
  }
}
