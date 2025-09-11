package ai.senscience.nexus.delta.plugins.storage.files.routes

import ai.senscience.nexus.delta.sdk.marshalling.QueryParamsUnmarshalling
import ai.senscience.nexus.delta.sdk.model.IdSegment
import org.apache.pekko.http.scaladsl.server.*
import org.apache.pekko.http.scaladsl.server.Directives.*

trait FileUriDirectives extends QueryParamsUnmarshalling {

  def storageParam: Directive[Tuple1[Option[IdSegment]]] = parameter("storage".as[IdSegment].?)

}

object FileUriDirectives extends FileUriDirectives
