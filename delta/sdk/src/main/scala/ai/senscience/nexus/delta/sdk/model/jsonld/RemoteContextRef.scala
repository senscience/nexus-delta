package ai.senscience.nexus.delta.sdk.model.jsonld

import ai.senscience.nexus.delta.rdf.IriOrBNode.{BNode, Iri}
import ai.senscience.nexus.delta.rdf.Vocabulary.contexts
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContext.StaticContext
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContext}
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.sdk.marshalling.HttpResponseFields
import ai.senscience.nexus.delta.sdk.model.jsonld.RemoteContextRef.ProjectRemoteContextRef.ResourceContext
import ai.senscience.nexus.delta.sdk.resolvers.ResolverContextResolution.ProjectRemoteContext
import ai.senscience.nexus.delta.sourcing.Serializer
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveConfiguredCodec
import io.circe.syntax.EncoderOps
import io.circe.{Codec, Encoder, Json, JsonObject}

/**
  * Reference to a remote context
  */
sealed trait RemoteContextRef extends Product with Serializable {

  /**
    * The iri it is bound to
    */
  def iri: Iri
}

object RemoteContextRef {

  def apply(remoteContexts: Map[Iri, RemoteContext]): Set[RemoteContextRef] =
    remoteContexts.foldLeft(Set.empty[RemoteContextRef]) {
      case (acc, (input, _: StaticContext))              => acc + StaticContextRef(input)
      case (acc, (input, context: ProjectRemoteContext)) =>
        acc + ProjectRemoteContextRef(input, ResourceContext(context.iri, context.project, context.rev))
      case (_, (_, context))                             =>
        throw new NotImplementedError(s"Case for '${context.getClass.getSimpleName}' has not been implemented.")
    }

  /**
    * A reference to a static remote context
    */
  final case class StaticContextRef(iri: Iri) extends RemoteContextRef

  /**
    * A reference to a context registered in a Nexus project
    * @param iri
    *   the resolved iri
    * @param resource
    *   the qualified reference to the Nexus resource
    */
  final case class ProjectRemoteContextRef(iri: Iri, resource: ResourceContext) extends RemoteContextRef

  object ProjectRemoteContextRef {
    final case class ResourceContext(id: Iri, project: ProjectRef, rev: Int)
  }

  given Codec.AsObject[RemoteContextRef] = {
    given Configuration                   = Serializer.circeConfiguration
    given Codec.AsObject[ResourceContext] = deriveConfiguredCodec[ResourceContext]
    deriveConfiguredCodec[RemoteContextRef]
  }

  given JsonLdEncoder[Set[RemoteContextRef]] = {
    given Encoder.AsObject[Set[RemoteContextRef]] = Encoder.AsObject.instance { remoteContexts =>
      JsonObject("remoteContexts" -> Json.arr(remoteContexts.map(_.asJson).toSeq*))
    }
    JsonLdEncoder.computeFromCirce(id = BNode.random, ctx = ContextValue(contexts.remoteContexts))
  }

  given HttpResponseFields[Set[RemoteContextRef]] = HttpResponseFields.defaultOk
}
