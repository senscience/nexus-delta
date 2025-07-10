package ai.senscience.nexus.delta.sdk.projects

import ai.senscience.nexus.delta.kernel.RetryStrategyConfig
import ai.senscience.nexus.delta.kernel.utils.Handlebars
import ai.senscience.nexus.delta.sdk.model.search.PaginationConfig
import ai.senscience.nexus.delta.sdk.projects.ProjectsConfig.PrefixConfig.PrefixIriTemplate
import ai.senscience.nexus.delta.sdk.projects.ProjectsConfig.{DeletionConfig, PrefixConfig}
import ai.senscience.nexus.delta.sdk.projects.model.PrefixIri
import ai.senscience.nexus.delta.sourcing.config.EventLogConfig
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import cats.syntax.all.*
import pureconfig.ConfigReader
import pureconfig.error.CannotConvert
import pureconfig.generic.semiauto.*

import scala.concurrent.duration.FiniteDuration

/**
  * Configuration for the Projects module.
  *
  * @param eventLog
  *   configuration of the event log
  * @param pagination
  *   configuration for how pagination should behave in listing operations
  * @param deletion
  *   the deletion configuration
  */
final case class ProjectsConfig(
    eventLog: EventLogConfig,
    prefix: PrefixConfig,
    pagination: PaginationConfig,
    deletion: DeletionConfig
)

object ProjectsConfig {

  final case class PrefixConfig(base: PrefixIriTemplate, vocab: PrefixIriTemplate)

  object PrefixConfig {

    val orgKey     = "org"
    val projectKey = "project"

    final case class PrefixIriTemplate private (value: String) extends AnyVal {
      def create(project: ProjectRef): PrefixIri = {
        val args = Map(orgKey -> project.organization.value, projectKey -> project.project.value)
        // The template has been tested at startup, it should be fine
        PrefixIri(Handlebars(value, args)).getOrElse(
          throw new IllegalStateException(
            s"'$value' is not a valid template, it should not happen but check the config."
          )
        )
      }
    }

    object PrefixIriTemplate {
      private val args = Map(orgKey -> "org", projectKey -> "proj")

      def unsafe(value: String): PrefixIriTemplate = PrefixIriTemplate(value)

      implicit final val prefixConfigTemplateReader: ConfigReader[PrefixIriTemplate] =
        ConfigReader.fromString { s =>
          PrefixIri(Handlebars(s, args)).bimap(
            err => CannotConvert(s, PrefixIriTemplate.getClass.getSimpleName, err.getMessage),
            _ => PrefixIriTemplate(s)
          )
        }
    }

    implicit final val prefixConfigReader: ConfigReader[PrefixConfig] = deriveReader[PrefixConfig]
  }

  /**
    * Configuration for project deletion
    * @param enabled
    *   if the project deletion is enabled
    * @param propagationDelay
    *   gives a delay for project deletion tasks to be taken into account, especially for views deprecation events to be
    *   acknowledged by coordinators
    * @param retryStrategy
    *   the retry strategy to apply when a project deletion fails
    */
  final case class DeletionConfig(
      enabled: Boolean,
      propagationDelay: FiniteDuration,
      retryStrategy: RetryStrategyConfig
  )

  object DeletionConfig {
    implicit final val deletionConfigReader: ConfigReader[DeletionConfig] =
      deriveReader[DeletionConfig]
  }

  implicit final val projectConfigReader: ConfigReader[ProjectsConfig] =
    deriveReader[ProjectsConfig]
}
