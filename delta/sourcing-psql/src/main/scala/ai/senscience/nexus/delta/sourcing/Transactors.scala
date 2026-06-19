package ai.senscience.nexus.delta.sourcing

import ai.senscience.nexus.delta.kernel.{Logger, Secret}
import ai.senscience.nexus.delta.sourcing.config.DatabaseConfig
import ai.senscience.nexus.delta.sourcing.config.DatabaseConfig.{DatabaseAccess, OpentelemetryConfig}
import ai.senscience.nexus.delta.sourcing.partition.PartitionStrategy
import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.zaxxer.hikari.HikariConfig
import io.opentelemetry.instrumentation.hikaricp.v3_0.HikariTelemetry
import org.typelevel.doobie.hikari.HikariTransactor
import org.typelevel.doobie.otel4s.{SpanNamer, TracedTransactor, TracingConfig}
import org.typelevel.doobie.util.transactor.Transactor
import org.typelevel.otel4s.oteljava.OtelJava
import org.typelevel.otel4s.semconv.attributes.DbAttributes
import org.typelevel.otel4s.trace.TracerProvider

import scala.concurrent.duration.*

/**
  * Allow to define different transactors (and connection pools) for the different query purposes
  */
final case class Transactors(
    read: Transactor[IO],
    write: Transactor[IO],
    streaming: Transactor[IO]
)

object Transactors {

  private val logger = Logger[Transactors]

  def test(
      host: String,
      port: Int,
      username: String,
      password: String,
      databaseName: String
  ): Resource[IO, Transactors] = {
    val access         = DatabaseAccess(host, port, 10)
    val databaseConfig = DatabaseConfig(
      access,
      access,
      access,
      PartitionStrategy.Hash(1),
      databaseName,
      username,
      Secret(password),
      tablesAutocreate = false,
      rewriteBatchInserts = true,
      5.seconds,
      OpentelemetryConfig.default
    )
    apply(databaseConfig, None)
  }

  def apply(config: DatabaseConfig, otelOpt: Option[OtelJava[IO]]): Resource[IO, Transactors] = {

    def jdbcUrl(access: DatabaseAccess, readOnly: Boolean) = {
      val baseUrl = s"jdbc:postgresql://${access.host}:${access.port}/${config.name}"
      if !readOnly && config.rewriteBatchInserts then s"$baseUrl?reWriteBatchedInserts=true"
      else baseUrl
    }

    def transactor(access: DatabaseAccess, readOnly: Boolean, poolName: String): Resource[IO, Transactor[IO]] = {
      val hikariConfig = new HikariConfig()
      hikariConfig.setJdbcUrl(jdbcUrl(access, readOnly))
      hikariConfig.setUsername(config.username)
      hikariConfig.setPassword(config.password.value)
      hikariConfig.setDriverClassName("org.postgresql.Driver")
      hikariConfig.setMaximumPoolSize(access.poolSize)
      hikariConfig.setPoolName(poolName)
      hikariConfig.setAutoCommit(false)
      hikariConfig.setReadOnly(readOnly)

      // Connection-pool metrics (db.client.connection.*) emitted via the HikariCP OpenTelemetry instrumentation.
      otelOpt.foreach { otel =>
        hikariConfig.setMetricsTrackerFactory(HikariTelemetry.create(otel.underlying).createMetricsTrackerFactory())
      }

      val logHandler = Some(QueryLogHandler(poolName, config.slowQueryThreshold))
      val base       = HikariTransactor.fromHikariConfig[IO](hikariConfig, logHandler)

      otelOpt match {
        case Some(otel) =>
          given TracerProvider[IO] = otel.tracerProvider
          val tracing              = TracingConfig
            .recommended(DbAttributes.DbSystemNameValue.Postgresql, config.name)
            .withCaptureQuery(config.otel.queryCapture)
            .withQueryAnalyzer(DoobieQueryAnalyzer)
            .withSpanNamer(SpanNamer.fromQueryMetadata.orElse(SpanNamer.fromAttribute(DbAttributes.DbQuerySummary)))
          base.evalMap(TracedTransactor.create[IO](_, tracing, logHandler))
        case None       =>
          base
      }
    }

    val read      = transactor(config.read, readOnly = true, poolName = "ReadPool")
    val write     = transactor(config.write, readOnly = false, poolName = "WritePool")
    val streaming = transactor(config.streaming, readOnly = true, poolName = "StreamingPool")

    (read, write, streaming)
      .mapN(Transactors(_, _, _))
      .evalTap { xas =>
        DDLLoader.setup(config.tablesAutocreate, config.partitionStrategy, xas)
      }
      .onError { case e =>
        Resource.eval(
          logger.error(e)("Error while initilizing database connections.")
        )
      }
  }
}
