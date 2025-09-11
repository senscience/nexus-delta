package ai.senscience.nexus.delta.sdk.directives

import ai.senscience.nexus.delta.kernel.search.Pagination.FromPagination
import ai.senscience.nexus.delta.kernel.search.TimeRange
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.*
import ai.senscience.nexus.delta.sdk.directives.UriDirectives.{extractHttp4sUri, iriSegment}
import ai.senscience.nexus.delta.sdk.error.ServiceError.ResourceNotFound
import ai.senscience.nexus.delta.sdk.implicits.*
import ai.senscience.nexus.delta.sdk.indexing.*
import ai.senscience.nexus.delta.sdk.marshalling.RdfMarshalling
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.model.search.{PaginationConfig, SearchResults}
import ai.senscience.nexus.delta.sdk.views.ViewRef
import ai.senscience.nexus.delta.sourcing.Scope
import ai.senscience.nexus.delta.sourcing.model.FailedElemLog.FailedElemData
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.projections.ProjectionSelector.{Name, ProjectId}
import ai.senscience.nexus.delta.sourcing.projections.{ProjectionErrors, ProjectionSelector, Projections}
import ai.senscience.nexus.delta.sourcing.query.SelectFilter
import cats.effect.IO
import cats.syntax.all.*
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route

trait ProjectionsDirectives {

  def statistics(scope: Scope, selectFilter: SelectFilter, projectionName: String): Route

  def statistics(project: ProjectRef, selectFilter: SelectFilter, projectionName: String): Route =
    statistics(Scope.Project(project), selectFilter, projectionName)

  def offset(projectionName: String): Route

  def scheduleRestart(projectionName: String, offset: Offset)(implicit subject: Subject): Route

  def indexingStatus(project: ProjectRef, selectFilter: SelectFilter, projectionName: String): Route

  def indexingErrors(view: ViewRef): Route =
    indexingErrors(view.project, view.viewId)

  def indexingErrors(project: ProjectRef, id: Iri): Route =
    indexingErrors(ProjectId(project, id))

  def indexingErrors(name: String): Route = indexingErrors(Name(name))

  def indexingErrors(selector: ProjectionSelector): Route
}

object ProjectionsDirectives extends RdfMarshalling {

  def apply(
      projections: Projections,
      projectionErrors: ProjectionErrors
  )(implicit baseUri: BaseUri, cr: RemoteContextResolution, ordering: JsonKeyOrdering): ProjectionsDirectives =
    new ProjectionsDirectives {

      implicit val paginationConfig: PaginationConfig = PaginationConfig(50, 1_000, 10_000)

      override def statistics(scope: Scope, selectFilter: SelectFilter, projectionName: String): Route =
        emit(projections.statistics(scope, selectFilter, projectionName))

      override def offset(projectionName: String): Route = emit(projections.offset(projectionName))

      override def scheduleRestart(projectionName: String, offset: Offset)(implicit subject: Subject): Route =
        emit(projections.scheduleRestart(projectionName, offset).as(offset))

      override def indexingStatus(project: ProjectRef, selectFilter: SelectFilter, projectionName: String): Route =
        (iriSegment & pathEndOrSingleSlash) { resourceId =>
          emitJson(
            projections
              .indexingStatus(project, selectFilter, projectionName, resourceId)(
                ResourceNotFound(resourceId, project)
              )
          )
        }

      override def indexingErrors(selector: ProjectionSelector): Route =
        (fromPaginated & timeRange("instant") & extractHttp4sUri & pathEndOrSingleSlash) {
          (pagination, timeRange, uri) =>
            implicit val searchJsonLdEncoder: JsonLdEncoder[FailedElemSearchResults] =
              failedElemSearchJsonLdEncoder(pagination, uri)
            emit(searchErrors(selector, pagination, timeRange))
        }

      private def searchErrors(
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

  def testEcho: ProjectionsDirectives = new ProjectionsDirectives {
    override def statistics(scope: Scope, selectFilter: SelectFilter, projectionName: String): Route =
      complete("indexing-statistics")

    override def offset(projectionName: String): Route = complete("offset")

    override def scheduleRestart(projectionName: String, offset: Offset)(implicit subject: Subject): Route =
      complete("schedule-restart")

    override def indexingStatus(project: ProjectRef, selectFilter: SelectFilter, projectionName: String): Route =
      complete("indexing-status")

    override def indexingErrors(selector: ProjectionSelector): Route = complete("indexing-errors")
  }
}
