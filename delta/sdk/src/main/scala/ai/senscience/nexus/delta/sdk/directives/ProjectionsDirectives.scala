package ai.senscience.nexus.delta.sdk.directives

import ai.senscience.nexus.delta.kernel.search.Pagination.FromPagination
import ai.senscience.nexus.delta.kernel.search.TimeRange
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.directives.DeltaDirectives.*
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
import org.typelevel.otel4s.trace.Tracer

trait ProjectionsDirectives {

  def statistics(scope: Scope, selectFilter: SelectFilter, projectionName: String)(using Tracer[IO]): Route

  def statistics(project: ProjectRef, selectFilter: SelectFilter, projectionName: String)(using Tracer[IO]): Route =
    statistics(Scope.Project(project), selectFilter, projectionName)

  def offset(projectionName: String)(using Tracer[IO]): Route

  def scheduleRestart(projectionName: String, offset: Offset)(using Subject, Tracer[IO]): Route

  def indexingStatus(project: ProjectRef, selectFilter: SelectFilter, projectionName: String)(using Tracer[IO]): Route

  def indexingErrors(view: ViewRef)(using Tracer[IO]): Route =
    indexingErrors(view.project, view.viewId)

  def indexingErrors(project: ProjectRef, id: Iri)(using Tracer[IO]): Route =
    indexingErrors(ProjectId(project, id))

  def indexingErrors(name: String)(using Tracer[IO]): Route = indexingErrors(Name(name))

  def indexingErrors(selector: ProjectionSelector)(using Tracer[IO]): Route
}

object ProjectionsDirectives extends RdfMarshalling {

  def apply(
      projections: Projections,
      projectionErrors: ProjectionErrors
  )(using BaseUri, RemoteContextResolution, JsonKeyOrdering): ProjectionsDirectives =
    new ProjectionsDirectives {

      private given PaginationConfig = PaginationConfig(50, 1_000, 10_000)

      override def statistics(scope: Scope, selectFilter: SelectFilter, projectionName: String)(using
          Tracer[IO]
      ): Route =
        emit(projections.statistics(scope, selectFilter, projectionName))

      override def offset(projectionName: String)(using Tracer[IO]): Route = emit(projections.offset(projectionName))

      override def scheduleRestart(projectionName: String, offset: Offset)(using Subject, Tracer[IO]): Route =
        emit(projections.scheduleRestart(projectionName, offset).as(offset))

      override def indexingStatus(project: ProjectRef, selectFilter: SelectFilter, projectionName: String)(using
          Tracer[IO]
      ): Route =
        (iriSegment & pathEndOrSingleSlash) { resourceId =>
          emitJson(
            projections
              .indexingStatus(project, selectFilter, projectionName, resourceId)(
                ResourceNotFound(resourceId, project)
              )
          )
        }

      override def indexingErrors(selector: ProjectionSelector)(using Tracer[IO]): Route =
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
    override def statistics(scope: Scope, selectFilter: SelectFilter, projectionName: String)(using Tracer[IO]): Route =
      complete("indexing-statistics")

    override def offset(projectionName: String)(using Tracer[IO]): Route = complete("offset")

    override def scheduleRestart(projectionName: String, offset: Offset)(using Subject, Tracer[IO]): Route =
      complete("schedule-restart")

    override def indexingStatus(project: ProjectRef, selectFilter: SelectFilter, projectionName: String)(using
        Tracer[IO]
    ): Route =
      complete("indexing-status")

    override def indexingErrors(selector: ProjectionSelector)(using Tracer[IO]): Route = complete("indexing-errors")
  }
}
