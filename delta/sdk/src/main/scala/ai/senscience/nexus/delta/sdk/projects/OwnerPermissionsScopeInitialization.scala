package ai.senscience.nexus.delta.sdk.projects

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.kernel.kamon.KamonMetricComponent
import ai.senscience.nexus.delta.kernel.syntax.*
import ai.senscience.nexus.delta.sdk.ScopeInitialization
import ai.senscience.nexus.delta.sdk.acls.Acls
import ai.senscience.nexus.delta.sdk.acls.model.{Acl, AclRejection}
import ai.senscience.nexus.delta.sdk.error.ServiceError.ScopeInitializationFailed
import ai.senscience.nexus.delta.sdk.identities.model.ServiceAccount
import ai.senscience.nexus.delta.sdk.organizations.model.Organization
import ai.senscience.nexus.delta.sdk.permissions.Permissions
import ai.senscience.nexus.delta.sdk.permissions.model.Permission
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.{EntityType, ProjectRef}
import cats.effect.IO

/**
  * The default creation of ACLs for newly created organizations and projects.
  *
  * @param appendAcls
  *   how to append acls
  * @param ownerPermissions
  *   the collection of permissions to be granted to the owner (creator)
  */
class OwnerPermissionsScopeInitialization(appendAcls: Acl => IO[Unit], ownerPermissions: Set[Permission])
    extends ScopeInitialization {

  implicit private val kamonComponent: KamonMetricComponent = KamonMetricComponent("ownerPermissions")

  private val logger = Logger[OwnerPermissionsScopeInitialization]

  override def onOrganizationCreation(
      organization: Organization,
      subject: Subject
  ): IO[Unit] =
    appendAcls(Acl(organization.label, subject -> ownerPermissions))
      .handleErrorWith {
        case _: AclRejection.IncorrectRev => IO.unit // acls are already set
        case rej                          =>
          val str = s"Failed to apply the owner permissions for org '${organization.label}' due to '${rej.getMessage}'."
          logger.error(str) >> IO.raiseError(ScopeInitializationFailed(str))
      }
      .span("setOrgPermissions")

  override def onProjectCreation(project: ProjectRef, subject: Subject): IO[Unit] =
    appendAcls(Acl(project, subject -> ownerPermissions))
      .handleErrorWith {
        case _: AclRejection.IncorrectRev => IO.unit // acls are already set
        case rej                          =>
          val str = s"Failed to apply the owner permissions for project '$project' due to '${rej.getMessage}'."
          logger.error(str) >> IO.raiseError(ScopeInitializationFailed(str))
      }
      .span("setProjectPermissions")

  override def entityType: EntityType = Permissions.entityType
}

object OwnerPermissionsScopeInitialization {

  /**
    * Create the [[OwnerPermissionsScopeInitialization]] from an acls instance
    *
    * @param acls
    *   the acls module
    * @param ownerPermissions
    *   the collection of permissions to be granted to the owner (creator)
    * @param serviceAccount
    *   the subject that will be recorded when performing the initialization
    */
  def apply(
      acls: Acls,
      ownerPermissions: Set[Permission],
      serviceAccount: ServiceAccount
  ): OwnerPermissionsScopeInitialization = {
    implicit val serviceAccountSubject: Subject = serviceAccount.subject
    new OwnerPermissionsScopeInitialization(acls.append(_, 0).void, ownerPermissions)
  }
}
