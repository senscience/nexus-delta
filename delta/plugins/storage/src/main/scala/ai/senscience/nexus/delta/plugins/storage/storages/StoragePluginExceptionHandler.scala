package ai.senscience.nexus.delta.plugins.storage.storages

import ai.senscience.nexus.delta.plugins.storage.files.model.FileRejection
import ai.senscience.nexus.delta.plugins.storage.files.model.FileRejection.FileNotFound
import ai.senscience.nexus.delta.plugins.storage.storages.model.StorageRejection
import ai.senscience.nexus.delta.plugins.storage.storages.model.StorageRejection.StorageNotFound
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.discardEntityAndForceEmit
import ai.senscience.nexus.delta.sdk.directives.Response.Reject
import ai.senscience.nexus.delta.sdk.marshalling.RdfExceptionHandler
import ai.senscience.nexus.delta.sdk.model.BaseUri
import org.apache.pekko.http.scaladsl.server.Directives.{handleExceptions, reject}
import org.apache.pekko.http.scaladsl.server.{Directive0, ExceptionHandler}

object StoragePluginExceptionHandler {

  private val rejectStoragePredicate: Throwable => Boolean = {
    case _: StorageNotFound => true
    case _                  => false
  }

  private val rejectFilePredicate: Throwable => Boolean = {
    case _: FileNotFound => true
    case _               => false
  }

  private def apply(implicit
      baseUri: BaseUri,
      cr: RemoteContextResolution,
      ordering: JsonKeyOrdering
  ): ExceptionHandler =
    ExceptionHandler {
      case err: StorageRejection if rejectStoragePredicate(err) => reject(Reject(err))
      case err: StorageRejection                                => discardEntityAndForceEmit(err)
      case err: FileRejection if rejectFilePredicate(err)       => reject(Reject(err))
      case err: FileRejection                                   => discardEntityAndForceEmit(err)
    }.withFallback(RdfExceptionHandler.apply)

  def handleStorageExceptions(implicit
      baseUri: BaseUri,
      cr: RemoteContextResolution,
      ordering: JsonKeyOrdering
  ): Directive0 = handleExceptions(StoragePluginExceptionHandler.apply)

}
