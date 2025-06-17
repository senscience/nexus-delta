package ai.senscience.nexus.delta.plugins.storage.storages

import ai.senscience.nexus.delta.plugins.storage.storages.model.Storage
import ai.senscience.nexus.delta.plugins.storage.storages.model.StorageRejection.StorageIsDeprecated
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sdk.acls.AclCheck
import ai.senscience.nexus.delta.sdk.error.ServiceError.AuthorizationFailed
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.permissions.model.Permission
import ai.senscience.nexus.delta.sourcing.model.ResourceRef.Latest
import ai.senscience.nexus.delta.sourcing.model.{ProjectRef, ResourceRef}
import cats.effect.IO

trait FetchStorage {

  /**
    * Attempts to fetch the storage in a read context and validates if the current user has access to it
    */
  def onRead(id: ResourceRef, project: ProjectRef)(implicit caller: Caller): IO[Storage]

  /**
    * Attempts to fetch the provided storage or the default one in a write context
    */
  def onWrite(id: Option[Iri], project: ProjectRef)(implicit
      caller: Caller
  ): IO[(ResourceRef.Revision, Storage)]
}

object FetchStorage {

  def apply(storages: Storages, aclCheck: AclCheck): FetchStorage = new FetchStorage {

    override def onRead(id: ResourceRef, project: ProjectRef)(implicit caller: Caller): IO[Storage] =
      storages.fetch(id, project).map(_.value).flatTap { storage =>
        validateAuth(project, storage.storageValue.readPermission)
      }

    override def onWrite(id: Option[Iri], project: ProjectRef)(implicit
        caller: Caller
    ): IO[(ResourceRef.Revision, Storage)] =
      for {
        storage <- id match {
                     case Some(id) => storages.fetch(Latest(id), project)
                     case None     => storages.fetchDefault(project)
                   }
        _       <- IO.raiseWhen(storage.deprecated)(StorageIsDeprecated(storage.id))
        _       <- validateAuth(project, storage.value.storageValue.writePermission)
      } yield ResourceRef.Revision(storage.id, storage.rev) -> storage.value

    private def validateAuth(project: ProjectRef, permission: Permission)(implicit c: Caller): IO[Unit] =
      aclCheck.authorizeForOr(project, permission)(AuthorizationFailed(project, permission))
  }

}
