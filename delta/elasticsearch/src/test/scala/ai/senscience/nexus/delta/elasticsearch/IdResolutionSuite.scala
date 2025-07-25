package ai.senscience.nexus.delta.elasticsearch

import ai.senscience.nexus.delta.elasticsearch.IdResolution.ResolutionResult.{MultipleResults, SingleResult}
import ai.senscience.nexus.delta.elasticsearch.IdResolutionSuite.searchResults
import ai.senscience.nexus.delta.elasticsearch.query.{MainIndexQuery, MainIndexRequest}
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.syntax.iriStringContextSyntax
import ai.senscience.nexus.delta.sdk.DataResource
import ai.senscience.nexus.delta.sdk.error.ServiceError.AuthorizationFailed
import ai.senscience.nexus.delta.sdk.generators.ResourceGen
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.model.search.{AggregationResult, SearchResults}
import ai.senscience.nexus.delta.sdk.permissions.model.Permission
import ai.senscience.nexus.delta.sdk.projects.ProjectScopeResolver
import ai.senscience.nexus.delta.sourcing.Scope
import ai.senscience.nexus.delta.sourcing.model.Identity.{Group, User}
import ai.senscience.nexus.delta.sourcing.model.{Label, ProjectRef, ResourceRef}
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.effect.IO
import cats.syntax.all.*
import io.circe.syntax.KeyOps
import io.circe.{Json, JsonObject}
import org.http4s.Query

class IdResolutionSuite extends NexusSuite with Fixtures {

  private val realm         = Label.unsafe("myrealm")
  private val alice: Caller = Caller(User("Alice", realm), Set(User("Alice", realm), Group("users", realm)))

  private val org = Label.unsafe("org")

  private val project1 = ProjectRef(org, Label.unsafe("proj"))

  private val org2     = Label.unsafe("org2")
  private val project2 = ProjectRef(org2, Label.unsafe("proj2"))

  private def projectResolver: ProjectScopeResolver = new ProjectScopeResolver {
    override def apply(scope: Scope, permission: Permission)(implicit caller: Caller): IO[Set[ProjectRef]] =
      IO.pure { Set(project1, project2) }

    override def access(scope: Scope, permission: Permission)(implicit
        caller: Caller
    ): IO[ProjectScopeResolver.PermissionAccess] = ???
  }

  private def mainIndexQuery(searchResults: SearchResults[JsonObject]): MainIndexQuery = {
    new MainIndexQuery {
      override def search(project: ProjectRef, query: JsonObject, qp: Query): IO[Json] = IO.pure(Json.Null)

      override def list(request: MainIndexRequest, projects: Set[ProjectRef]): IO[SearchResults[JsonObject]] =
        IO.pure(searchResults)

      override def aggregate(request: MainIndexRequest, projects: Set[ProjectRef]): IO[AggregationResult] =
        IO.pure(AggregationResult(0, JsonObject.empty))
    }
  }

  private val iri = iri"https://bbp.epfl.ch/data/resource"

  private val successId      = nxv + "success"
  private val successContent =
    ResourceGen.jsonLdContent(successId, project1, jsonContentOf("resources/resource.json", "id" -> successId))

  private def fetchResource =
    (_: ResourceRef, _: ProjectRef) => IO.pure(successContent.some)

  private val res = JsonObject("@id" := iri, "_project" := project1)

  test("No listing results lead to AuthorizationFailed") {
    val noListingResults = mainIndexQuery(searchResults(Seq.empty))
    IdResolution(projectResolver, noListingResults, fetchResource)
      .apply(iri)(alice)
      .intercept[AuthorizationFailed]
  }

  test("Single listing result leads to the resource being fetched") {
    val singleListingResult = mainIndexQuery(searchResults(Seq(res)))
    IdResolution(projectResolver, singleListingResult, fetchResource)
      .apply(iri)(alice)
      .assertEquals(SingleResult(ResourceRef(iri), project1, successContent))
  }

  test("Multiple listing results lead to search results") {
    val searchRes            = searchResults(Seq(res, res))
    val multipleQueryResults = mainIndexQuery(searchRes)
    IdResolution(projectResolver, multipleQueryResults, fetchResource)
      .apply(iri)(alice)
      .assertEquals(MultipleResults(searchRes))
  }

}

object IdResolutionSuite {
  def asResourceF(resourceRef: ResourceRef, projectRef: ProjectRef)(implicit
      rcr: RemoteContextResolution
  ): DataResource = {
    val resource = ResourceGen.resource(resourceRef.iri, projectRef, Json.obj())
    ResourceGen.resourceFor(resource)
  }

  private def searchResults(jsons: Seq[JsonObject]): SearchResults[JsonObject] =
    SearchResults(jsons.size.toLong, jsons)
}
