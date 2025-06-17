package ai.senscience.nexus.delta.sdk.generators

import ai.senscience.nexus.delta.sdk.model.Fetch.FetchF
import ai.senscience.nexus.delta.sdk.resolvers.ResolverResolution.ResourceResolution
import ai.senscience.nexus.delta.sdk.resolvers.ResourceResolution
import ai.senscience.nexus.delta.sdk.resolvers.model.ResolverRejection.ResolverNotFound
import ai.senscience.nexus.delta.sourcing.model.{Identity, ProjectRef, ResourceRef}
import cats.effect.IO
import ch.epfl.bluebrain.nexus.delta.rdf.IriOrBNode.Iri
import ch.epfl.bluebrain.nexus.delta.rdf.Vocabulary.nxv

object ResourceResolutionGen {

  /**
    * Create a resource resolution based on a single in-project resolver
    * @param projectRef
    *   the project
    * @param fetchResource
    *   how to fetch the resource
    */
  def singleInProject[R](
      projectRef: ProjectRef,
      fetchResource: (ResourceRef, ProjectRef) => FetchF[R]
  ): ResourceResolution[R] = {
    val resolver = ResolverGen.inProject(nxv + "in-project", projectRef)

    val checkAcls     = (_: ProjectRef, _: Set[Identity]) => IO.pure(false)
    val listResolvers = (_: ProjectRef) => IO.pure(List(resolver))
    val fetchResolver = (resolverId: Iri, p: ProjectRef) =>
      IO.raiseUnless(resolverId == resolver.id && p == resolver.project)(ResolverNotFound(resolverId, p)).as(resolver)

    ResourceResolution(
      checkAcls,
      listResolvers,
      fetchResolver,
      fetchResource,
      excludeDeprecated = false
    )
  }
}
