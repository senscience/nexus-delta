package ai.senscience.nexus.delta.sdk.identities.model

import ai.senscience.nexus.delta.rdf.Vocabulary.contexts
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.sdk.implicits.{*, given}
import ai.senscience.nexus.delta.sdk.instances.IdentityInstances
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sourcing.model.Identity
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import io.circe.{Encoder, JsonObject}

/**
  * Data type that represents the collection of identities of the client. A caller must be either Anonymous or a
  * specific User.
  *
  * @param subject
  *   the subject identity of the caller (User or Anonymous)
  * @param identities
  *   the full collection of identities, including the subject
  */
final case class Caller private (subject: Subject, identities: Set[Identity])

object Caller {

  /**
    * The constant anonymous caller.
    */
  val Anonymous: Caller = Caller(Identity.Anonymous, Set(Identity.Anonymous))

  def apply(subject: Subject, identities: Set[Identity] = Set.empty): Caller =
    if identities.contains(subject) then new Caller(subject, identities)
    else new Caller(subject, identities + subject)

  given BaseUri => Encoder.AsObject[Caller] = {
    val identityDecoder = IdentityInstances.identityEncoder
    Encoder.AsObject.instance[Caller] { caller =>
      JsonObject.singleton(
        "identities",
        Encoder.encodeList(using identityDecoder)(caller.identities.toList.sortBy(_.asIri.toString))
      )
    }
  }

  private val context                    = ContextValue(contexts.metadata, contexts.identities)
  given BaseUri => JsonLdEncoder[Caller] = JsonLdEncoder.computeFromCirce(context)

}
