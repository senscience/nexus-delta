package ai.senscience.nexus.delta.plugins.storage

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.kernel.kamon.KamonMetricComponent
import ai.senscience.nexus.delta.plugins.storage.storages.model.StorageFields
import ai.senscience.nexus.delta.plugins.storage.storages.model.StorageFields.{DiskStorageFields, S3StorageFields}
import ai.senscience.nexus.delta.plugins.storage.storages.model.StorageRejection.ResourceAlreadyExists
import ai.senscience.nexus.delta.plugins.storage.storages.{defaultS3StorageId, defaultStorageId, Storages}
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sdk.error.ServiceError.ScopeInitializationFailed
import ai.senscience.nexus.delta.sdk.identities.model.{Caller, ServiceAccount}
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sdk.organizations.model.Organization
import ai.senscience.nexus.delta.sdk.{Defaults, ScopeInitialization}
import ai.senscience.nexus.delta.sourcing.model.{EntityType, Identity, ProjectRef}
import cats.effect.IO

/**
  * The default creation of the default disk storage as part of the project initialization.
  *
  * @param storages
  *   the storages module
  * @param serviceAccount
  *   the subject that will be recorded when performing the initialization
  * @param defaultStorageId
  *   the id to use for the default storage to be created
  * @param defaultStorageFields
  *   the default value for the storage fields
  */
class StorageScopeInitialization(
    storages: Storages,
    serviceAccount: ServiceAccount,
    defaultStorageId: Iri,
    defaultStorageFields: StorageFields
) extends ScopeInitialization {

  private val logger                                        = Logger[StorageScopeInitialization]
  implicit private val kamonComponent: KamonMetricComponent = KamonMetricComponent(Storages.entityType.value)

  implicit private val caller: Caller = serviceAccount.caller

  override def onProjectCreation(project: ProjectRef, subject: Identity.Subject): IO[Unit] =
    storages
      .create(defaultStorageId, project, defaultStorageFields)
      .void
      .handleErrorWith {
        case _: ResourceAlreadyExists => IO.unit // nothing to do, storage already exits
        case rej                      =>
          val str =
            s"Failed to create the default ${defaultStorageFields.tpe} for project '$project' due to '${rej.getMessage}'."
          logger.error(str) >> IO.raiseError(ScopeInitializationFailed(str))
      }
      .span("createDefaultStorage")

  override def onOrganizationCreation(
      organization: Organization,
      subject: Identity.Subject
  ): IO[Unit] = IO.unit

  override def entityType: EntityType = Storages.entityType
}

object StorageScopeInitialization {

  /**
    * Creates a [[StorageScopeInitialization]] that creates a default DiskStorage with the provided default
    * name/description
    */
  def apply(storages: Storages, serviceAccount: ServiceAccount, defaults: Defaults): StorageScopeInitialization = {
    val defaultFields: DiskStorageFields = DiskStorageFields(
      name = Some(defaults.name),
      description = Some(defaults.description),
      default = true,
      volume = None,
      readPermission = None,
      writePermission = None,
      maxFileSize = None
    )
    new StorageScopeInitialization(storages, serviceAccount, defaultStorageId, defaultFields)
  }

  /**
    * Creates a [[StorageScopeInitialization]] that creates a default S3Storage with the provided default fields
    */
  def s3(
      storages: Storages,
      serviceAccount: ServiceAccount,
      defaultFields: S3StorageFields
  ): StorageScopeInitialization =
    new StorageScopeInitialization(storages, serviceAccount, defaultS3StorageId, defaultFields)

}
