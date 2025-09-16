package ai.senscience.nexus.delta.sdk

import ai.senscience.nexus.delta.kernel.RetryStrategyConfig
import ai.senscience.nexus.delta.kernel.cache.CacheConfig
import ai.senscience.nexus.delta.sdk.fusion.FusionConfig
import ai.senscience.nexus.delta.sdk.model.search.PaginationConfig
import ai.senscience.nexus.delta.sdk.projects.ProjectsConfig
import ai.senscience.nexus.delta.sourcing.config.{EventLogConfig, QueryConfig}
import ai.senscience.nexus.delta.sourcing.query.RefreshStrategy
import org.http4s.syntax.literals.uri

import scala.concurrent.duration.*

trait ConfigFixtures {

  def cacheConfig: CacheConfig = CacheConfig(10, 5.minutes)

  def queryConfig: QueryConfig = QueryConfig(5, RefreshStrategy.Stop)

  def eventLogConfig: EventLogConfig = EventLogConfig(queryConfig, 5.seconds)

  def pagination: PaginationConfig =
    PaginationConfig(
      defaultSize = 30,
      sizeLimit = 100,
      fromLimit = 10000
    )

  def fusionConfig: FusionConfig =
    FusionConfig(uri"https://bbp.epfl.ch/nexus/web/", enableRedirects = true, uri"https://bbp.epfl.ch")

  def deletionConfig: ProjectsConfig.DeletionConfig = ProjectsConfig.DeletionConfig(
    enabled = true,
    1.second,
    RetryStrategyConfig.AlwaysGiveUp
  )
}
