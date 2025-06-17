package ai.senscience.nexus.delta.sdk.resolvers

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.{contexts, nxv, schemas}
import ai.senscience.nexus.delta.rdf.jsonld.context.JsonLdContext.keywords
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContext.StaticContext
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolutionError.RemoteContextNotAccessible
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import ai.senscience.nexus.delta.rdf.jsonld.{CompactedJsonLd, ExpandedJsonLd}
import ai.senscience.nexus.delta.sdk.generators.ResourceResolutionGen
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sdk.model.Fetch.FetchF
import ai.senscience.nexus.delta.sdk.model.{ResourceAccess, ResourceF}
import ai.senscience.nexus.delta.sdk.resolvers.ResolverContextResolution.ProjectRemoteContext
import ai.senscience.nexus.delta.sdk.resources.model.Resource
import ai.senscience.nexus.delta.sourcing.model.Identity.User
import ai.senscience.nexus.delta.sourcing.model.ResourceRef.Latest
import ai.senscience.nexus.delta.sourcing.model.{Label, ProjectRef, ResourceRef, Tags}
import cats.effect.IO
import ch.epfl.bluebrain.nexus.testkit.mu.NexusSuite
import io.circe.Json
import io.circe.syntax.*

import java.time.Instant

class ResolverContextResolutionSuite extends NexusSuite {

  private val metadataContext = jsonContentOf("contexts/metadata.json").topContextValueOrEmpty

  val rcr: RemoteContextResolution =
    RemoteContextResolution.fixed(contexts.metadata -> metadataContext)

  private val alice                = User("alice", Label.unsafe("wonderland"))
  implicit val aliceCaller: Caller = Caller(alice, Set(alice))

  private val project = ProjectRef.unsafe("org", "project")

  private val resourceId = nxv + "id"
  private val context    = (nxv + "context").asJson

  private val resource = ResourceF(
    id = resourceId,
    access = ResourceAccess(uri"/id"),
    rev = 5,
    types = Set(nxv + "Resource"),
    deprecated = false,
    createdAt = Instant.now(),
    createdBy = alice,
    updatedAt = Instant.now(),
    updatedBy = alice,
    schema = Latest(schemas + "ResourceExample"),
    value = Resource(
      nxv + "example1",
      project,
      Tags.empty,
      Latest(nxv + "schema"),
      Json.obj(keywords.context -> context),
      CompactedJsonLd.empty,
      ExpandedJsonLd.empty
    )
  )

  def fetchResource: (ResourceRef, ProjectRef) => FetchF[Resource] = { (r: ResourceRef, p: ProjectRef) =>
    (r, p) match {
      case (Latest(id), `project`) if resourceId == id => IO.pure(Some(resource))
      case _                                           => IO.none
    }
  }

  private val resourceResolution = ResourceResolutionGen.singleInProject(project, fetchResource)

  private val resolverContextResolution = ResolverContextResolution(rcr, resourceResolution)

  private def resolve(iri: Iri) = resolverContextResolution(project).resolve(iri)

  test("Resolve correctly static contexts") {
    val expected = StaticContext(contexts.metadata, metadataContext)
    resolve(contexts.metadata).assertEquals(expected)
  }

  test("Resolve correctly a resource context") {
    val expected = ProjectRemoteContext(resourceId, project, 5, ContextValue(context))
    resolve(resourceId).assertEquals(expected)
  }

  test("Fail is applying for an unknown resource") {
    resolve(nxv + "xxx").intercept[RemoteContextNotAccessible]
  }
}
