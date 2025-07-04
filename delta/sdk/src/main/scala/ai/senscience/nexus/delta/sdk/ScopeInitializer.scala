package ai.senscience.nexus.delta.sdk

import ai.senscience.nexus.delta.sdk.error.ServiceError.ScopeInitializationFailed
import ai.senscience.nexus.delta.sdk.organizations.model.OrganizationRejection.OrganizationInitializationFailed
import ai.senscience.nexus.delta.sdk.projects.ScopeInitializationErrorStore
import ai.senscience.nexus.delta.sdk.projects.ScopeInitializationErrorStore.noopStore
import ai.senscience.nexus.delta.sdk.projects.model.ProjectRejection.ProjectInitializationFailed
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import cats.effect.IO
import cats.implicits.*

trait ScopeInitializer {

  /** Execute the actions necessary at org creation */
  def initializeOrganization(
      organizationResource: OrganizationResource
  )(implicit caller: Subject): IO[Unit]

  /** Execute the actions necessary at project creation */
  def initializeProject(
      project: ProjectRef
  )(implicit caller: Subject): IO[Unit]

}

object ScopeInitializer {

  /**
    * Construct a [[ScopeInitializer]] out of the provided [[ScopeInitialization]]
    *
    * @param scopeInitializations
    *   the set of initializations to execute
    * @param errorStore
    *   the store for initialization errors
    * @return
    */
  def apply(
      scopeInitializations: Set[ScopeInitialization],
      errorStore: => ScopeInitializationErrorStore
  ): ScopeInitializer =
    new ScopeInitializer {

      override def initializeOrganization(
          organizationResource: OrganizationResource
      )(implicit caller: Subject): IO[Unit] =
        scopeInitializations
          .runAll { init =>
            init.onOrganizationCreation(organizationResource.value, caller)
          }
          .adaptError { case e: ScopeInitializationFailed =>
            OrganizationInitializationFailed(e)
          }

      override def initializeProject(
          project: ProjectRef
      )(implicit caller: Subject): IO[Unit] = {
        scopeInitializations
          .runAll { init =>
            init
              .onProjectCreation(project, caller)
              .onError {
                case e: ScopeInitializationFailed => errorStore.save(init.entityType, project, e)
                case _                            => IO.unit
              }
          }
          .adaptError { case e: ScopeInitializationFailed =>
            ProjectInitializationFailed(e)
          }
      }

    }

  implicit private class SetOps[A](set: Set[A]) {

    /**
      * Runs all the provided effects in parallel, but does not cancel the rest if one fails. In this implementation,
      * the error being raised is the first one that occurs. This is needed because if we use parTraverse, the rest of
      * the effects will be canceled if one fails.
      * @param f
      *   effect to run on each element of the set
      */
    def runAll(f: A => IO[Unit]): IO[Unit] =
      set.toList.parFoldMapA(f(_).attempt).flatMap(IO.fromEither)
  }

  /** A constructor for tests that does not store initialization errors */
  def withoutErrorStore(
      scopeInitializations: Set[ScopeInitialization]
  ): ScopeInitializer =
    apply(scopeInitializations, noopStore)

  /** An initializer that does not perform any operation */
  def noop: ScopeInitializer = withoutErrorStore(Set.empty)
}
