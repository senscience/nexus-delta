package ai.senscience.nexus.delta.sdk.syntax

import ai.senscience.nexus.delta.sdk.marshalling.HttpResponseFields
import org.apache.pekko.http.scaladsl.model.{HttpHeader, StatusCode}

trait HttpResponseFieldsSyntax {
  implicit final def httpResponseFieldsSyntax[A](value: A): HttpResponseFieldsOps[A] = new HttpResponseFieldsOps(value)
}

final class HttpResponseFieldsOps[A](private val value: A) extends AnyVal {

  /**
    * @return
    *   the HTTP status code extracted from the current value using the [[HttpResponseFields]]
    */
  def status(implicit responseFields: HttpResponseFields[A]): StatusCode =
    responseFields.statusFrom(value)

  /**
    * @return
    *   the HTTP headers extracted from the current value using the [[HttpResponseFields]]
    */
  def headers(implicit responseFields: HttpResponseFields[A]): Seq[HttpHeader] =
    responseFields.headersFrom(value)

  /**
    * @return
    *   the entity for the etag support in conditional requests
    */
  def entityTag(implicit responseFields: HttpResponseFields[A]): Option[String] =
    responseFields.entityTag(value)

}
