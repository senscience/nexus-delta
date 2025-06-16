package ai.senscience.nexus.delta.plugins.storage.storages

import ai.senscience.nexus.delta.plugins.storage.files.model.FileRejection
import ai.senscience.nexus.delta.plugins.storage.storages.model.StorageRejection
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.discardEntityAndForceEmit
import ai.senscience.nexus.delta.sdk.marshalling.RdfExceptionHandler
import ai.senscience.nexus.delta.sdk.model.BaseUri
import akka.http.scaladsl.server.Directives.handleExceptions
import akka.http.scaladsl.server.{Directive0, ExceptionHandler}
import ch.epfl.bluebrain.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ch.epfl.bluebrain.nexus.delta.rdf.utils.JsonKeyOrdering

object StoragePluginExceptionHandler {

  def apply(implicit baseUri: BaseUri, cr: RemoteContextResolution, ordering: JsonKeyOrdering): ExceptionHandler =
    ExceptionHandler {
      case err: StorageRejection => discardEntityAndForceEmit(err)
      case err: FileRejection    => discardEntityAndForceEmit(err)
    }.withFallback(RdfExceptionHandler.apply)

  def handleStorageExceptions(implicit
      baseUri: BaseUri,
      cr: RemoteContextResolution,
      ordering: JsonKeyOrdering
  ): Directive0 = handleExceptions(StoragePluginExceptionHandler.apply)

}
