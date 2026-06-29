package ai.senscience.nexus.delta.sourcing

import io.opentelemetry.instrumentation.api.incubator.semconv.db.{SqlDialect, SqlQueryAnalyzer}
import org.typelevel.doobie.otel4s.QueryAnalyzer
import org.typelevel.doobie.otel4s.QueryAnalyzer.QueryMetadata

import scala.annotation.nowarn

/**
  * Adapts OpenTelemetry Java's [[SqlQueryAnalyzer]] to a doobie-otel4s [[QueryAnalyzer]], so that DB spans are named by
  * their query summary (e.g. `SELECT public.scoped_states`) rather than the generic JDBC operation (`executeQuery`).
  *
  * doobie-otel4s ships only [[QueryAnalyzer.noop]]; reusing the OpenTelemetry SQL analyzer gives us a battle-tested SQL
  * parser (operation, target table, low-cardinality summary) while keeping the tracing otel4s-native.
  *
  * @see
  *   https://typelevel.org/doobie/docs/20-Otel4s-Tracing.html
  */
object DoobieQueryAnalyzer extends QueryAnalyzer {

  // `create(true)` enables sanitization; the analyzer keeps a bounded internal cache and is safe to share.
  private val delegate: SqlQueryAnalyzer = SqlQueryAnalyzer.create(true)
  // Postgres uses double quotes for identifiers (and single quotes for string literals).
  private val dialect: SqlDialect        = SqlDialect.DOUBLE_QUOTES_ARE_IDENTIFIERS

  // `getOperationName`/`getCollectionName` are deprecated (to become package-private in OTel 3.0)
  @nowarn("cat=deprecation")
  def analyze(sql: String): Option[QueryMetadata] =
    Option(delegate.analyzeWithSummary(sql, dialect)).map { q =>
      QueryMetadata(
        queryText = Option(q.getQueryText),
        operationName = Option(q.getOperationName),
        collectionName = Option(q.getCollectionName),
        storedProcedureName = Option(q.getStoredProcedureName),
        querySummary = Option(q.getQuerySummary)
      )
    }
}
