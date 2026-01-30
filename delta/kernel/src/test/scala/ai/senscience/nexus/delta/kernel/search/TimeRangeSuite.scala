package ai.senscience.nexus.delta.kernel.search

import ai.senscience.nexus.delta.kernel.search.TimeRange.*
import ai.senscience.nexus.delta.kernel.search.TimeRange.ParseError.{InvalidFormat, InvalidRange, InvalidValue}
import munit.{FunSuite, Location}

import java.time.Instant

class TimeRangeSuite extends FunSuite {

  private val december_2020_string = "2020-12-20T20:20:00Z"
  private val december_2020        = Instant.parse(december_2020_string)
  private val april_2022_string    = "2022-04-10T04:20:00Z"
  private val april_2022           = Instant.parse(april_2022_string)

  private def assertLeft(value: String, error: ParseError)(using Location): Unit =
    assertEquals(parse(value), Left(error))

  private def assertRight(value: String, result: TimeRange)(using Location): Unit =
    assertEquals(parse(value), Right(result))

  List(
    "",
    "FAIL",
    ".."
  ).foreach { badInput =>
    test(s"Fail to parse '$badInput' for an invalid format") {
      assertLeft(badInput, InvalidFormat(badInput))
    }
  }

  List(
    "BAD..*"                      -> "BAD",
    s"BAD..$december_2020_string" -> "BAD",
    "*..BAD"                      -> "BAD",
    s"$december_2020_string..BAD" -> "BAD",
    "2020..*"                     -> "2020",
    "2020-12-20..*"               -> "2020-12-20",       // Missing the time
    "2020-12-20T20:20..*"         -> "2020-12-20T20:20", // Missing the second and the timezone
    "2020-12-20T20:20Z..*"        -> "2020-12-20T20:20Z" // Missing the timezone
  ).foreach { case (badInput, badValue) =>
    test(s"Fail to parse '$badInput' for an invalid value") {
      assertLeft(badInput, InvalidValue(badValue))
    }
  }

  test("Parse '*..*' as anytime") {
    assertEquals(parse("*..*"), Right(Anytime))
  }

  test(s"Parse as a before '$december_2020_string'") {
    assertRight(s"*..$december_2020_string", Before(december_2020))
  }

  test(s"Parse as a after '$december_2020_string'") {
    assertRight(s"$december_2020_string..*", After(december_2020))
  }

  test(s"Parse as between '$december_2020_string' and '$april_2022_string'") {
    val expected = Between.unsafe(december_2020, april_2022)
    assertRight(s"$december_2020_string..$april_2022_string", expected)
  }

  test("Fail to parse as between as limits are equal") {
    assertLeft(
      s"$december_2020_string..$december_2020_string",
      InvalidRange(december_2020_string, december_2020_string)
    )
  }

  test(s"Fail to parse as between as '$april_2022_string' > '$december_2020_string'") {
    assertLeft(s"$april_2022_string..$december_2020_string", InvalidRange(april_2022_string, december_2020_string))
  }

}
