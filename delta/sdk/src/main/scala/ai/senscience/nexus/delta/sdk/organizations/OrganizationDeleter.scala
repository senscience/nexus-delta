package ai.senscience.nexus.delta.sdk.organizations

import ai.senscience.nexus.delta.sdk.acls.Acls
import ai.senscience.nexus.delta.sdk.acls.model.AclAddress
import ai.senscience.nexus.delta.sdk.organizations.model.OrganizationRejection.OrganizationNonEmpty
import ai.senscience.nexus.delta.sdk.projects.Projects
import ai.senscience.nexus.delta.sourcing.Scope.Org
import ai.senscience.nexus.delta.sourcing.model.Label
import ai.senscience.nexus.delta.sourcing.partition.DatabasePartitioner
import cats.effect.IO
import ch.epfl.bluebrain.nexus.delta.kernel.Logger

trait OrganizationDeleter {
  def apply(org: Label): IO[Unit]
}

object OrganizationDeleter {

  def apply(
      acls: Acls,
      orgs: Organizations,
      projects: Projects,
      databasePartitioner: DatabasePartitioner
  ): OrganizationDeleter = {
    def hasAnyProject(org: Label): IO[Boolean] =
      projects.currentRefs(Org(org)).find(_.organization == org).compile.last.map(_.isDefined)
    def compiledDeletionTask(org: Label)       =
      acls.purge(AclAddress.fromOrg(org)) >> databasePartitioner.onDeleteOrg(org) >> orgs.purge(org)
    apply(hasAnyProject, compiledDeletionTask)
  }

  def apply(hasAnyProject: Label => IO[Boolean], deletionTask: Label => IO[Unit]): OrganizationDeleter =
    new OrganizationDeleter {

      private val logger = Logger[OrganizationDeleter]

      def apply(org: Label): IO[Unit] =
        hasAnyProject(org).flatMap {
          case true  =>
            logger.error(s"Failed to delete empty organization $org") >> IO.raiseError(OrganizationNonEmpty(org))
          case false =>
            logger.info(s"Deleting empty organization $org") >> deletionTask(org)
        }
    }
}
