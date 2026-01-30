package ai.senscience.nexus.testkit.mu

import munit.{Assertions, Location}

trait StringAssertions { self: Assertions =>

  extension (obtained: String) {

    private def sort(value: String) = value.split("\n").filterNot(_.trim.isEmpty).sorted.toList

    def equalLinesUnordered(expected: String)(using Location): Unit = {
      val obtainedSorted = sort(obtained)
      val expectedSorted = sort(expected)

      assertEquals(
        obtainedSorted,
        expectedSorted,
        s"""
           |Both strings are different.
           |Diff:
           |${obtainedSorted.diff(expectedSorted).mkString("\n")}
           |Obtained:
           |${obtainedSorted.mkString("\n")}
           |Expected:
           |${expectedSorted.mkString("\n")}
           |""".stripMargin
      )
    }
  }

}
