package ai.senscience.nexus.delta.sdk.resolvers.model

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.contexts
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.rdf.syntax.{jsonObjectOpsSyntax, jsonOpsSyntax}
import ai.senscience.nexus.delta.sdk.OrderingFields
import ai.senscience.nexus.delta.sdk.instances.IdentityInstances
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.resolvers.model.ResolverValue.{CrossProjectValue, InProjectValue}
import ai.senscience.nexus.delta.sourcing.model.{Identity, ProjectRef}
import io.circe.syntax.*
import io.circe.{Encoder, Json}

/**
  * Enumeration of Resolver types.
  */
sealed trait Resolver extends Product with Serializable {

  /**
    * @return
    *   the resolver id
    */
  def id: Iri

  /**
    * @return
    *   a reference to the project that the resolver belongs to
    */
  def project: ProjectRef

  /**
    * @return
    *   the resolver priority
    */
  def priority: Priority

  /**
    * @return
    *   the representation of the resolver as posted by the subject
    */
  def source: Json

  /**
    * @return
    *   The underlying resolver value
    */
  def value: ResolverValue
}

object Resolver {

  /**
    * A resolver that looks only within its own project.
    */
  final case class InProjectResolver(
      id: Iri,
      project: ProjectRef,
      value: InProjectValue,
      source: Json
  ) extends Resolver {
    override def priority: Priority = value.priority
  }

  /**
    * A resolver that can look across several projects.
    */
  final case class CrossProjectResolver(
      id: Iri,
      project: ProjectRef,
      value: CrossProjectValue,
      source: Json
  ) extends Resolver {
    override def priority: Priority = value.priority
  }

  val context: ContextValue = ContextValue(contexts.resolvers)

  implicit def resolverEncoder(implicit baseUri: BaseUri): Encoder.AsObject[Resolver] = {
    implicit val identityEncoder: Encoder[Identity] = IdentityInstances.identityEncoder
    Encoder.AsObject.instance { r =>
      r.value.asJsonObject.addContext(r.source.topContextValueOrEmpty.excludeRemoteContexts.contextObj)
    }
  }

  implicit def resolverJsonLdEncoder(implicit baseUri: BaseUri): JsonLdEncoder[Resolver] =
    JsonLdEncoder.computeFromCirce(_.id, context)

  implicit val resolverOrderingFields: OrderingFields[Resolver] = OrderingFields.empty

}
