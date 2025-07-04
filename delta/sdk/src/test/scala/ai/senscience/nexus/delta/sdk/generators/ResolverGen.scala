package ai.senscience.nexus.delta.sdk.generators

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sdk.ResolverResource
import ai.senscience.nexus.delta.sdk.resolvers.model.Resolver.InProjectResolver
import ai.senscience.nexus.delta.sdk.resolvers.model.ResolverValue.InProjectValue
import ai.senscience.nexus.delta.sdk.resolvers.model.{Priority, ResolverState, ResolverValue}
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sourcing.model.Identity.{Anonymous, Subject}
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import io.circe.Json

import java.time.Instant

object ResolverGen {

  /**
    * Generate an in-project resolver
    * @param id
    *   the id of the resolver
    * @param project
    *   the project of the resolver
    */
  def inProject(id: Iri, project: ProjectRef, priority: Int = 20): InProjectResolver =
    InProjectResolver(
      id,
      project,
      InProjectValue(Priority.unsafe(priority)),
      Json.obj()
    )

  /**
    * Generate a ResolverResource for the given parameters
    */
  def resolverResourceFor(
      id: Iri,
      project: ProjectRef,
      value: ResolverValue,
      source: Json,
      rev: Int = 1,
      subject: Subject = Anonymous,
      deprecated: Boolean = false
  ): ResolverResource =
    ResolverState(
      id: Iri,
      project,
      value,
      source,
      rev,
      deprecated,
      Instant.EPOCH,
      subject,
      Instant.EPOCH,
      subject
    ).toResource

  /**
    * Generate a valid json source from resolver id and value
    */
  def sourceFrom(id: Iri, resolverValue: ResolverValue): Json =
    ResolverValue.generateSource(id, resolverValue)

  /**
    * Generate a valid json source from resolver value and omitting an id
    */
  def sourceWithoutId(resolverValue: ResolverValue): Json =
    ResolverValue.generateSource(nxv + "test", resolverValue).removeAllKeys("@id")

}
