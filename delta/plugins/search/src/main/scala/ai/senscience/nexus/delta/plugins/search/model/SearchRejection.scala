package ai.senscience.nexus.delta.plugins.search.model

import ai.senscience.nexus.delta.kernel.error.Rejection
import ai.senscience.nexus.delta.kernel.utils.ClassUtils
import ai.senscience.nexus.delta.rdf.Vocabulary
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue
import ai.senscience.nexus.delta.rdf.jsonld.context.JsonLdContext.keywords
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.sdk.marshalling.HttpResponseFields
import ai.senscience.nexus.delta.sourcing.model.Label
import io.circe.syntax.*
import io.circe.{Encoder, JsonObject}
import org.apache.pekko.http.scaladsl.model.StatusCodes

/**
  * Enumeration of search rejection types.
  *
  * @param reason
  *   a descriptive message as to why the rejection occurred
  */
sealed abstract class SearchRejection(val reason: String) extends Rejection

object SearchRejection {

  /**
    * Signals a rejection caused when interacting with the elasticserch client
    */
  final case class UnknownSuite(value: Label) extends SearchRejection(s"The suite '$value' can't be found.")

  private[plugins] given Encoder.AsObject[SearchRejection] =
    Encoder.AsObject.instance { r =>
      val tpe = ClassUtils.simpleName(r)
      JsonObject(keywords.tpe -> tpe.asJson, "reason" -> r.reason.asJson)
    }

  given JsonLdEncoder[SearchRejection] =
    JsonLdEncoder.computeFromCirce(ContextValue(Vocabulary.contexts.error))

  given HttpResponseFields[SearchRejection] =
    HttpResponseFields { case UnknownSuite(_) =>
      StatusCodes.NotFound
    }
}
