package ai.senscience.nexus.delta.plugins.search

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.kernel.syntax.*
import ai.senscience.nexus.delta.plugins.compositeviews.CompositeViews
import ai.senscience.nexus.delta.plugins.compositeviews.model.CompositeViewRejection.ViewAlreadyExists
import ai.senscience.nexus.delta.plugins.search.model.SearchConfig.IndexingConfig
import ai.senscience.nexus.delta.plugins.search.model.defaultViewId
import ai.senscience.nexus.delta.sdk.error.ServiceError.ScopeInitializationFailed
import ai.senscience.nexus.delta.sdk.identities.model.ServiceAccount
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.organizations.model.Organization
import ai.senscience.nexus.delta.sdk.{Defaults, ScopeInitialization}
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.{EntityType, Identity, ProjectRef}
import cats.effect.IO

final class SearchScopeInitialization(
    views: CompositeViews,
    config: IndexingConfig,
    serviceAccount: ServiceAccount,
    defaults: Defaults
)(implicit baseUri: BaseUri)
    extends ScopeInitialization {

  private val logger = Logger[SearchScopeInitialization]

  implicit private val serviceAccountSubject: Subject = serviceAccount.subject

  override def onProjectCreation(
      project: ProjectRef,
      subject: Identity.Subject
  ): IO[Unit] = {
    views
      .create(defaultViewId, project, SearchViewFactory(defaults, config))
      .void
      .handleErrorWith {
        case _: ViewAlreadyExists => IO.unit
        case rej                  =>
          val str =
            s"Failed to create the search view for project '$project' due to '${rej.getMessage}'."
          logger.error(str) >> IO.raiseError(ScopeInitializationFailed(str))
      }
      .named("createSearchView", "search")
  }

  override def onOrganizationCreation(
      organization: Organization,
      subject: Identity.Subject
  ): IO[Unit] = IO.unit

  override def entityType: EntityType = EntityType("searchView")
}
