package ai.senscience.nexus.delta.sourcing

import ai.senscience.nexus.delta.kernel.Secret
import ai.senscience.nexus.delta.sourcing.config.DatabaseConfig
import ai.senscience.nexus.delta.sourcing.config.DatabaseConfig.{DatabaseAccess, OpentelemetryConfig}
import ai.senscience.nexus.delta.sourcing.partition.PartitionStrategy
import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.zaxxer.hikari.HikariConfig
import doobie.hikari.HikariTransactor
import doobie.otel4s.hikari.TelemetryHikariTransactor
import doobie.util.transactor.Transactor
import org.typelevel.otel4s.oteljava.OtelJava

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

      val logHandler = Some(QueryLogHandler(poolName, config.slowQueryThreshold))

      otelOpt match {
        case Some(otel) =>
          val otelConfig = config.otel
          TelemetryHikariTransactor.fromHikariConfig[IO](
            otel = otel.underlying,
            config = hikariConfig,
            logHandler = Some(QueryLogHandler(poolName, config.slowQueryThreshold)),
            statementInstrumenterEnabled = otelConfig.statementInstrumenterEnabled,
            statementSanitizationEnabled = otelConfig.statementSanitizationEnabled,
            captureQueryParameters = otelConfig.captureQueryParameters,
            transactionInstrumenterEnabled = otelConfig.transactionInstrumenterEnabled
          )
        case None       =>
          HikariTransactor.fromHikariConfig(hikariConfig, logHandler)
      }
    }

    val read      = transactor(config.read, readOnly = true, poolName = "ReadPool")
    val write     = transactor(config.write, readOnly = false, poolName = "WritePool")
    val streaming = transactor(config.streaming, readOnly = true, poolName = "StreamingPool")

    (read, write, streaming).mapN(Transactors(_, _, _)).evalTap { xas =>
      DDLLoader.setup(config.tablesAutocreate, config.partitionStrategy, xas)
    }
  }
}
