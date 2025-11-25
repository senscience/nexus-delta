package ai.senscience.nexus.delta.sdk.resolvers.model

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.schemas
import ai.senscience.nexus.delta.sdk.ResolverResource
import ai.senscience.nexus.delta.sdk.model.{ResourceAccess, ResourceF}
import ai.senscience.nexus.delta.sdk.resolvers.model.Resolver.{CrossProjectResolver, InProjectResolver}
import ai.senscience.nexus.delta.sdk.resolvers.model.ResolverValue.{CrossProjectValue, InProjectValue}
import ai.senscience.nexus.delta.sourcing.Serializer
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.ResourceRef.Latest
import ai.senscience.nexus.delta.sourcing.model.{ProjectRef, ResourceRef}
import ai.senscience.nexus.delta.sourcing.state.State.ScopedState
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveConfiguredCodec
import io.circe.{Codec, Json}

import java.time.Instant

/**
  * State for an existing in project resolver
  * @param id
  *   the id of the resolver
  * @param project
  *   the project it belongs to
  * @param value
  *   additional fields to configure the resolver
  * @param source
  *   the representation of the resolver as posted by the subject
  * @param rev
  *   the current state revision
  * @param deprecated
  *   the current state deprecation status
  * @param createdAt
  *   the instant when the resource was created
  * @param createdBy
  *   the subject that created the resource
  * @param updatedAt
  *   the instant when the resource was last updated
  * @param updatedBy
  *   the subject that last updated the resource
  */
final case class ResolverState(
    id: Iri,
    project: ProjectRef,
    value: ResolverValue,
    source: Json,
    rev: Int,
    deprecated: Boolean,
    createdAt: Instant,
    createdBy: Subject,
    updatedAt: Instant,
    updatedBy: Subject
) extends ScopedState {

  def schema: ResourceRef = Latest(schemas.resolvers)

  override def types: Set[Iri] = value.tpe.types

  def resolver: Resolver = {
    value match {
      case inProjectValue: InProjectValue       =>
        InProjectResolver(
          id = id,
          project = project,
          value = inProjectValue,
          source = source
        )
      case crossProjectValue: CrossProjectValue =>
        CrossProjectResolver(
          id = id,
          project = project,
          value = crossProjectValue,
          source = source
        )
    }
  }

  def toResource: ResolverResource =
    ResourceF(
      id = id,
      access = ResourceAccess.resolver(project, id),
      rev = rev,
      types = value.tpe.types,
      deprecated = deprecated,
      createdAt = createdAt,
      createdBy = createdBy,
      updatedAt = updatedAt,
      updatedBy = updatedBy,
      schema = schema,
      value = resolver
    )
}

object ResolverState {

  val serializer: Serializer[Iri, ResolverState] = {
    import ai.senscience.nexus.delta.sdk.resolvers.model.IdentityResolution.Database.given
    import ai.senscience.nexus.delta.sourcing.model.Identity.Database.given
    given Configuration                 = Serializer.circeConfiguration
    given Codec.AsObject[ResolverValue] = deriveConfiguredCodec[ResolverValue]
    given Codec.AsObject[ResolverState] = deriveConfiguredCodec[ResolverState]
    Serializer.dropNullsInjectType()
  }
}
