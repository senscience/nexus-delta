package ai.senscience.nexus.delta.sdk.indexing

import ai.senscience.nexus.delta.kernel.search.Pagination.FromPagination
import ai.senscience.nexus.delta.kernel.search.TimeRange
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sdk.model.search.SearchResults
import ai.senscience.nexus.delta.sdk.views.ViewRef
import ai.senscience.nexus.delta.sourcing.model.FailedElemLogRow.FailedElemData
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.projections.ProjectionErrors
import cats.syntax.all.*
import cats.effect.IO

trait ProjectionErrorsSearch {

  def apply(view: ViewRef, pagination: FromPagination, timeRange: TimeRange): IO[SearchResults[FailedElemData]] =
    apply(view.project, view.viewId, pagination, timeRange)

  def apply(
      project: ProjectRef,
      id: Iri,
      pagination: FromPagination,
      timeRange: TimeRange
  ): IO[SearchResults[FailedElemData]]

}

object ProjectionErrorsSearch {

  def apply(projectionErrors: ProjectionErrors): ProjectionErrorsSearch = new ProjectionErrorsSearch {

    override def apply(
        project: ProjectRef,
        id: Iri,
        pagination: FromPagination,
        timeRange: TimeRange
    ): IO[SearchResults[FailedElemData]] = {
      for {
        results <- projectionErrors.list(project, id, pagination, timeRange)
        count   <- projectionErrors.count(project, id, timeRange)
      } yield SearchResults(
        count,
        results.map {
          _.failedElemData
        }
      )
    }.widen[SearchResults[FailedElemData]]
  }

}
