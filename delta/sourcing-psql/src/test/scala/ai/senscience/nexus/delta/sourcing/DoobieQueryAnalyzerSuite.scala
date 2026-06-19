package ai.senscience.nexus.delta.sourcing

import munit.FunSuite

class DoobieQueryAnalyzerSuite extends FunSuite {

  // `analyzeWithSummary` populates `querySummary` (operation + target) per stable semconv; that is what
  // `SpanNamer.fromQueryMetadata` uses to name the span.
  private def check(sql: String, operation: String, table: String): Unit = {
    val summary = DoobieQueryAnalyzer.analyze(sql).flatMap(_.querySummary)
    assert(
      summary.exists(s => s.contains(operation) && s.contains(table)),
      s"expected summary containing '$operation' and '$table' for: $sql, got $summary"
    )
  }

  test("derive operation and table for a SELECT") {
    check("SELECT type, id FROM public.scoped_states WHERE org = ? AND project = ?", "SELECT", "scoped_states")
  }

  test("derive operation and table for a parenthesised UNION SELECT") {
    val sql =
      """((SELECT type FROM public.scoped_states WHERE type IN (?, ?))
        |UNION ALL
        |(SELECT type FROM public.scoped_tombstones WHERE type IN (?, ?))
        |ORDER BY ordering) LIMIT ?""".stripMargin
    check(sql, "SELECT", "scoped_states")
  }

  test("derive operation and table for an INSERT") {
    check("INSERT INTO public.scoped_events (org, project, value) VALUES (?, ?, ?)", "INSERT", "scoped_events")
  }

  test("derive operation and table for an UPDATE") {
    check("UPDATE public.projects SET value = ? WHERE id = ?", "UPDATE", "projects")
  }

  test("derive operation and table for a DELETE") {
    check("DELETE FROM public.scoped_tombstones WHERE org = ? AND project = ?", "DELETE", "scoped_tombstones")
  }
}
