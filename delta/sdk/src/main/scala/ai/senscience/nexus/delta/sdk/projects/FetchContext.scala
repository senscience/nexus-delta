package ai.senscience.nexus.delta.sdk.projects

import ai.senscience.nexus.delta.kernel.cache.{CacheConfig, LocalCache}
import ai.senscience.nexus.delta.sdk.organizations.FetchActiveOrganization
import ai.senscience.nexus.delta.sdk.organizations.model.OrganizationRejection.OrganizationIsDeprecated
import ai.senscience.nexus.delta.sdk.projects.model.ProjectRejection.{ProjectIsDeprecated, ProjectIsMarkedForDeletion, ProjectNotFound}
import ai.senscience.nexus.delta.sdk.projects.model.{ApiMappings, ProjectContext, ProjectState}
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sourcing.Transactors
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.state.ScopedStateGet
import cats.effect.IO
import doobie.syntax.all.*
import doobie.{Get, Put}
import org.typelevel.otel4s.trace.Tracer

/**
  * Define the rules to fetch project context for read and write operations
  */
abstract class FetchContext { self =>

  /**
    * The default api mappings
    */
  def defaultApiMappings: ApiMappings

  /**
    * Fetch a context for a read operation
    * @param ref
    *   the project to fetch the context from
    */
  def onRead(ref: ProjectRef): IO[ProjectContext]

  /**
    * Fetch context for a create operation
    * @param ref
    *   the project to fetch the context from
    */
  def onCreate(ref: ProjectRef): IO[ProjectContext] = onModify(ref)

  /**
    * Fetch context for a modify operation
    * @param ref
    *   the project to fetch the context from
    */
  def onModify(ref: ProjectRef): IO[ProjectContext]

}

object FetchContext {

  final case class ProjectStatus(
      orgDeprecated: Boolean,
      projectDeprecated: Boolean,
      projectMarkedForDeletion: Boolean,
      context: ProjectContext
  )

  def apply(dam: ApiMappings, cacheConfig: CacheConfig, xas: Transactors)(using Tracer[IO]): IO[FetchContext] =
    LocalCache[ProjectRef, ProjectStatus](cacheConfig).map { cache =>
      given Put[ProjectRef]   = ProjectState.serializer.putId
      given Get[ProjectState] = ProjectState.serializer.getValue

      val fetchActiveOrg = FetchActiveOrganization(xas)

      def fetchProjectStatus(ref: ProjectRef): IO[Option[ProjectStatus]] =
        for {
          orgDeprecated <- fetchActiveOrg(ref.organization).redeem(_ => true, _ => false)
          project       <- ScopedStateGet.latest[ProjectRef, ProjectState](Projects.entityType, ref, ref).transact(xas.write)
        } yield project.map { p =>
          ProjectStatus(
            orgDeprecated = orgDeprecated,
            projectDeprecated = p.deprecated,
            projectMarkedForDeletion = p.markedForDeletion,
            context = ProjectContext(dam + p.apiMappings, p.base, p.vocab, p.enforceSchema)
          )
        }

      apply(
        dam,
        (project: ProjectRef) => cache.getOrElseAttemptUpdate(project, fetchProjectStatus(project))
      )
    }

  /**
    * Constructs a fetch context
    * @param dam
    *   the default api mappings defined by Nexus
    * @param fetchProjectStatus
    *   fetches the project status
    */
  def apply(
      dam: ApiMappings,
      fetchProjectStatus: ProjectRef => IO[Option[ProjectStatus]]
  )(using Tracer[IO]): FetchContext =
    new FetchContext {

      override def defaultApiMappings: ApiMappings = dam

      override def onRead(ref: ProjectRef): IO[ProjectContext] =
        fetchProjectStatus(ref)
          .flatMap {
            case None                                            => IO.raiseError(ProjectNotFound(ref))
            case Some(status) if status.projectMarkedForDeletion => IO.raiseError(ProjectIsMarkedForDeletion(ref))
            case Some(status)                                    => IO.pure(status.context)
          }
          .surround("fetchProjectContextRead")

      override def onModify(ref: ProjectRef): IO[ProjectContext] =
        fetchProjectStatus(ref)
          .flatMap {
            case None                                            => IO.raiseError(ProjectNotFound(ref))
            case Some(status) if status.projectMarkedForDeletion => IO.raiseError(ProjectIsMarkedForDeletion(ref))
            case Some(status) if status.orgDeprecated            => IO.raiseError(OrganizationIsDeprecated(ref.organization))
            case Some(status) if status.projectDeprecated        => IO.raiseError(ProjectIsDeprecated(ref))
            case Some(status)                                    => IO.pure(status.context)
          }
          .surround("fetchProjectContextWrite")

      override def onCreate(ref: ProjectRef): IO[ProjectContext] = onModify(ref)
    }
}
