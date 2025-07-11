package ai.senscience.nexus.delta.plugins.blazegraph

object BlazegraphErrorParser {

  /**
    * Attempts to extract the malformed query message from the raw exception
    */
  private def parseMalformedQueryException(rawError: String): Option[String] = {
    val javaExecutionExceptionMatcher = "java.util.concurrent.ExecutionException: "
    val malformedSegmentMatcher       = "java.util.concurrent.ExecutionException: org.openrdf.query.MalformedQueryException: "

    rawError.linesIterator
      .find(_.contains(malformedSegmentMatcher))
      .map(str => str.replace(javaExecutionExceptionMatcher, ""))
      .map(str => parseExpectedTokens(rawError).map(s => s"$str $s").getOrElse(str))
  }

  /**
    * Attempts to extract the expected tokens from the raw exception
    */
  private def parseExpectedTokens(rawError: String): Option[String] = {
    val wasExpectingOneOfMatcher = "Was expecting one of:"
    val errorLines               = rawError.linesIterator.toList
    val index                    = errorLines.indexWhere(_.startsWith(wasExpectingOneOfMatcher))

    Option
      .when(index != -1) {
        errorLines
          .drop(index + 1)
          .takeWhile(_.trim.nonEmpty)
          .map(_.replace("...", "").trim)
          .mkString(", ")
      }
      .map { expectedTokens =>
        s"$wasExpectingOneOfMatcher $expectedTokens."
      }
  }

  /**
    * Attempts to parse the raw error message. If it cannot be parsed the raw error is returned.
    */
  def parse(rawError: String): String =
    parseMalformedQueryException(rawError).getOrElse(rawError)
}
