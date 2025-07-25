package ai.senscience.nexus.delta.elasticsearch

import ai.senscience.nexus.delta.elasticsearch.IdResolution.ResolutionResult
import ai.senscience.nexus.delta.elasticsearch.IdResolution.ResolutionResult.{MultipleResults, SingleResult}
import ai.senscience.nexus.delta.elasticsearch.model.ResourcesSearchParams
import ai.senscience.nexus.delta.elasticsearch.query.{MainIndexQuery, MainIndexRequest}
import ai.senscience.nexus.delta.kernel.search.Pagination.FromPagination
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.contexts
import ai.senscience.nexus.delta.rdf.jsonld.api.{JsonLdApi, JsonLdOptions}
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.rdf.jsonld.{CompactedJsonLd, ExpandedJsonLd}
import ai.senscience.nexus.delta.sdk.error.ServiceError.AuthorizationFailed
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.jsonld.JsonLdContent
import ai.senscience.nexus.delta.sdk.marshalling.HttpResponseFields
import ai.senscience.nexus.delta.sdk.model.ResourceF.*
import ai.senscience.nexus.delta.sdk.model.search.SearchResults.searchResultsJsonLdEncoder
import ai.senscience.nexus.delta.sdk.model.search.{SearchResults, SortList}
import ai.senscience.nexus.delta.sdk.model.{BaseUri, ResourceF}
import ai.senscience.nexus.delta.sdk.permissions.Permissions.resources
import ai.senscience.nexus.delta.sdk.projects.ProjectScopeResolver
import ai.senscience.nexus.delta.sourcing.Scope
import ai.senscience.nexus.delta.sourcing.model.{ProjectRef, ResourceRef}
import cats.effect.IO
import io.circe.JsonObject

trait IdResolution {

  /**
    * Attempts to resolve the provided identifier across projects that the caller has access to
    *
    *   - If the query response is empty, leads to AuthorizationFailed.
    *   - If the query returns a single result, attempts to fetch the resource
    *   - If there are multiple results, they are returned as [[SearchResults]]
    *
    * @param iri
    *   identifier of the resource to be resolved
    * @param caller
    *   user having requested the resolution
    */
  def apply(iri: Iri)(implicit caller: Caller): IO[ResolutionResult]
}

object IdResolution {

  sealed trait ResolutionResult

  object ResolutionResult {

    final case class SingleResult[A](id: ResourceRef, project: ProjectRef, content: JsonLdContent[A])
        extends ResolutionResult

    case class MultipleResults(searchResults: SearchResults[JsonObject]) extends ResolutionResult

    private val searchJsonLdEncoder: JsonLdEncoder[SearchResults[JsonObject]] =
      searchResultsJsonLdEncoder(ContextValue(contexts.search))

    implicit def resultJsonLdEncoder(implicit baseUri: BaseUri): JsonLdEncoder[ResolutionResult] =
      new JsonLdEncoder[ResolutionResult] {

        private def encoder[A](value: JsonLdContent[A])(implicit baseUri: BaseUri): JsonLdEncoder[ResourceF[A]] = {
          implicit val encoder: JsonLdEncoder[A] = value.encoder
          resourceFAJsonLdEncoder[A](ContextValue.empty)
        }

        override def context(value: ResolutionResult): ContextValue = value match {
          case SingleResult(_, _, content)    => encoder(content).context(content.resource)
          case MultipleResults(searchResults) => searchJsonLdEncoder.context(searchResults)
        }

        override def expand(
            value: ResolutionResult
        )(implicit opts: JsonLdOptions, api: JsonLdApi, rcr: RemoteContextResolution): IO[ExpandedJsonLd] =
          value match {
            case SingleResult(_, _, content)    => encoder(content).expand(content.resource)
            case MultipleResults(searchResults) => searchJsonLdEncoder.expand(searchResults)
          }

        override def compact(
            value: ResolutionResult
        )(implicit opts: JsonLdOptions, api: JsonLdApi, rcr: RemoteContextResolution): IO[CompactedJsonLd] =
          value match {
            case SingleResult(_, _, content)    => encoder(content).compact(content.resource)
            case MultipleResults(searchResults) => searchJsonLdEncoder.compact(searchResults)
          }
      }

    implicit val resultHttpResponseFields: HttpResponseFields[ResolutionResult] = HttpResponseFields.defaultOk

  }

  def apply(
      projectScopeResolver: ProjectScopeResolver,
      mainIndexQuery: MainIndexQuery,
      fetchResource: (ResourceRef, ProjectRef) => IO[Option[JsonLdContent[?]]]
  ): IdResolution = new IdResolution {

    override def apply(iri: Iri)(implicit caller: Caller): IO[ResolutionResult] = {
      val locate  = ResourcesSearchParams(id = Some(iri))
      val request = MainIndexRequest(locate, FromPagination(0, 10000), SortList.empty)

      def fetchSingleResult: ProjectRef => IO[ResolutionResult] = { projectRef =>
        val resourceRef = ResourceRef(iri)
        fetchResource(resourceRef, projectRef)
          .map {
            _.map(SingleResult(resourceRef, projectRef, _))
          }
          .flatMap {
            case Some(result) => IO.pure(result)
            case None         =>
              IO.raiseError(new IllegalStateException("Resource found in ES payload but could not be fetched."))
          }
      }

      for {
        projects      <- projectScopeResolver(Scope.Root, resources.read)
        searchResults <- mainIndexQuery.list(request, projects)
        result        <- searchResults.results match {
                           case Nil         => IO.raiseError(AuthorizationFailed("No resource matches the provided id."))
                           case Seq(result) => projectRefFromSource(result.source).flatMap(fetchSingleResult)
                           case _           => IO.pure(MultipleResults(searchResults))
                         }
      } yield result
    }

    /** Extract the _project field of a given [[JsonObject]] as projectRef */
    private def projectRefFromSource(source: JsonObject) = {
      val projectOpt = source("_project").flatMap(_.as[ProjectRef].toOption)
      IO.fromOption(projectOpt)(new IllegalStateException("Could not read '_project' field as project reference."))
    }
  }
}
