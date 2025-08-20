package ai.senscience.nexus.delta.sdk.directives

import ai.senscience.nexus.delta.kernel.search.Pagination.FromPagination
import ai.senscience.nexus.delta.kernel.search.TimeRange
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.{emit, fromPaginated, timeRange}
import ai.senscience.nexus.delta.sdk.directives.UriDirectives.extractHttp4sUri
import ai.senscience.nexus.delta.sdk.indexing.{failedElemSearchJsonLdEncoder, FailedElemSearchResults}
import ai.senscience.nexus.delta.sdk.marshalling.RdfMarshalling
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.model.search.{PaginationConfig, SearchResults}
import ai.senscience.nexus.delta.sdk.views.ViewRef
import ai.senscience.nexus.delta.sourcing.model.FailedElemLog.FailedElemData
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.projections.ProjectionSelector.{Name, ProjectId}
import ai.senscience.nexus.delta.sourcing.projections.{ProjectionErrors, ProjectionSelector}
import akka.http.scaladsl.server.Directives.{complete, pathEndOrSingleSlash}
import akka.http.scaladsl.server.Route
import cats.effect.IO
import cats.syntax.all.*

trait ProjectionsDirectives {

  def indexingErrors(view: ViewRef): Route =
    indexingErrors(view.project, view.viewId)

  def indexingErrors(project: ProjectRef, id: Iri): Route =
    indexingErrors(ProjectId(project, id))

  def indexingErrors(name: String): Route = indexingErrors(Name(name))

  def indexingErrors(selector: ProjectionSelector): Route
}

object ProjectionsDirectives extends RdfMarshalling {

  def apply(
      projectionErrors: ProjectionErrors
  )(implicit baseUri: BaseUri, cr: RemoteContextResolution, ordering: JsonKeyOrdering): ProjectionsDirectives =
    new ProjectionsDirectives {

      implicit val paginationConfig: PaginationConfig = PaginationConfig(50, 1_000, 10_000)

      def indexingErrors(selector: ProjectionSelector): Route =
        (fromPaginated & timeRange("instant") & extractHttp4sUri & pathEndOrSingleSlash) {
          (pagination, timeRange, uri) =>
            implicit val searchJsonLdEncoder: JsonLdEncoder[FailedElemSearchResults] =
              failedElemSearchJsonLdEncoder(pagination, uri)
            emit(search(selector, pagination, timeRange))
        }

      private def search(
          selector: ProjectionSelector,
          pagination: FromPagination,
          timeRange: TimeRange
      ): IO[SearchResults[FailedElemData]] = {
        for {
          results <- projectionErrors.list(selector, pagination, timeRange)
          count   <- projectionErrors.count(selector, timeRange)
        } yield SearchResults(
          count,
          results.map {
            _.failedElemData
          }
        )
      }.widen[SearchResults[FailedElemData]]

    }

  def testEcho: ProjectionsDirectives =
    (_: ProjectionSelector) => complete("indexing-errors")
}
