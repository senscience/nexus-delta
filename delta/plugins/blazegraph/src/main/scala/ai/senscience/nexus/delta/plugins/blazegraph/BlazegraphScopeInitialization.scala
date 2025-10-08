package ai.senscience.nexus.delta.plugins.blazegraph

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.plugins.blazegraph.model.BlazegraphViewRejection.ResourceAlreadyExists
import ai.senscience.nexus.delta.plugins.blazegraph.model.BlazegraphViewValue.IndexingBlazegraphViewValue
import ai.senscience.nexus.delta.plugins.blazegraph.model.{defaultViewId, permissions}
import ai.senscience.nexus.delta.sdk.error.ServiceError.ScopeInitializationFailed
import ai.senscience.nexus.delta.sdk.identities.model.ServiceAccount
import ai.senscience.nexus.delta.sdk.organizations.model.Organization
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sdk.{Defaults, ScopeInitialization}
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.{EntityType, Identity, IriFilter, ProjectRef}
import cats.effect.IO
import org.typelevel.otel4s.trace.Tracer

/**
  * The default creation of the default SparqlView as part of the project initialization.
  *
  * @param views
  *   the BlazegraphViews module
  * @param serviceAccount
  *   the subject that will be recorded when performing the initialization
  * @param defaults
  *   default name and description for the view
  */
class BlazegraphScopeInitialization(
    views: BlazegraphViews,
    serviceAccount: ServiceAccount,
    defaults: Defaults
)(using Tracer[IO])
    extends ScopeInitialization {

  private val logger                                  = Logger[BlazegraphScopeInitialization]
  implicit private val serviceAccountSubject: Subject = serviceAccount.subject

  private def defaultValue: IndexingBlazegraphViewValue = IndexingBlazegraphViewValue(
    name = Some(defaults.name),
    description = Some(defaults.description),
    resourceSchemas = IriFilter.None,
    resourceTypes = IriFilter.None,
    resourceTag = None,
    includeMetadata = true,
    includeDeprecated = true,
    permission = permissions.query
  )

  override def onProjectCreation(project: ProjectRef, subject: Identity.Subject): IO[Unit] =
    views
      .create(defaultViewId, project, defaultValue)
      .void
      .handleErrorWith {
        case _: ResourceAlreadyExists => IO.unit // nothing to do, view already exits
        case rej                      =>
          val str =
            s"Failed to create the default SparqlView for project '$project' due to '${rej.getMessage}'."
          logger.error(str) >> IO.raiseError(ScopeInitializationFailed(str))
      }
      .surround("createDefaultSparqlView")

  override def onOrganizationCreation(
      organization: Organization,
      subject: Identity.Subject
  ): IO[Unit] = IO.unit

  override def entityType: EntityType = BlazegraphViews.entityType

}
