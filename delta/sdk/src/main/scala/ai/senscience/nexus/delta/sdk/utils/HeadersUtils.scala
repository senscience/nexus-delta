package ai.senscience.nexus.delta.sdk.utils

import org.apache.pekko.http.scaladsl.model.headers.RawHeader
import org.apache.pekko.http.scaladsl.model.{ContentType, HttpHeader, MediaRanges, MediaType}
import org.apache.pekko.http.scaladsl.server.MediaTypeNegotiator

object HeadersUtils {

  def contentType(value: ContentType) =
    RawHeader("Content-Type", value.toString)

  /**
    * Extracts the first mediaType found in the ''Accept'' Http request header that matches the ''serviceMediaTypes''.
    * If the Accept header does not match any of the service supported ''mediaTypes'', return None
    */
  def findFirst(headers: Seq[HttpHeader], serviceMediaTypes: Seq[MediaType]): Option[MediaType] = {
    val ct       = new MediaTypeNegotiator(headers)
    val accepted = if ct.acceptedMediaRanges.isEmpty then List(MediaRanges.`*/*`) else ct.acceptedMediaRanges
    accepted.foldLeft[Option[MediaType]](None) {
      case (s @ Some(_), _) => s
      case (None, mr)       => serviceMediaTypes.find(mt => mr.matches(mt))
    }
  }

  /**
    * Extracts the mediaTypes found in the ''Accept'' Http request and tries to match it to the passed ''mediaType''
    */
  def matches(headers: Seq[HttpHeader], mediaType: MediaType): Boolean =
    findFirst(headers, Seq(mediaType)).nonEmpty

}
