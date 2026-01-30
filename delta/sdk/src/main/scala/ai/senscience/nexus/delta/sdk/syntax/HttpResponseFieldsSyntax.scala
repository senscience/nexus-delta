package ai.senscience.nexus.delta.sdk.syntax

import ai.senscience.nexus.delta.sdk.marshalling.HttpResponseFields
import org.apache.pekko.http.scaladsl.model.{HttpHeader, StatusCode}

trait HttpResponseFieldsSyntax {

  extension [A](value: A) {

    /**
      * @return
      *   the HTTP status code extracted from the current value using the [[HttpResponseFields]]
      */
    def status(using responseFields: HttpResponseFields[A]): StatusCode =
      responseFields.statusFrom(value)

    /**
      * @return
      *   the HTTP headers extracted from the current value using the [[HttpResponseFields]]
      */
    def headers(using responseFields: HttpResponseFields[A]): Seq[HttpHeader] =
      responseFields.headersFrom(value)

    /**
      * @return
      *   the entity for the etag support in conditional requests
      */
    def entityTag(using responseFields: HttpResponseFields[A]): Option[String] =
      responseFields.entityTag(value)
  }
}
