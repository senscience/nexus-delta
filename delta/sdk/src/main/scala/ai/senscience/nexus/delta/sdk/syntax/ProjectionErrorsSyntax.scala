package ai.senscience.nexus.delta.sdk.syntax

import ai.senscience.nexus.delta.kernel.search.Pagination.FromPagination
import ai.senscience.nexus.delta.kernel.search.TimeRange
import ai.senscience.nexus.delta.sdk.model.search.SearchResults
import ai.senscience.nexus.delta.sdk.syntax.ProjectionErrorsSyntax.ProjectionErrorsOps
import ai.senscience.nexus.delta.sdk.views.ViewRef
import ai.senscience.nexus.delta.sourcing.model.FailedElemLogRow.FailedElemData
import ai.senscience.nexus.delta.sourcing.projections.ProjectionErrors
import cats.effect.IO
import cats.syntax.all.*

/**
  * Allows to extend the methods from [[ProjectionErrors]] by adding higher-level methods
  */
trait ProjectionErrorsSyntax {

  implicit def projectionErrorsOps(projectionErrors: ProjectionErrors): ProjectionErrorsOps = new ProjectionErrorsOps(
    projectionErrors
  )
}

object ProjectionErrorsSyntax {

  final class ProjectionErrorsOps(val projectionErrors: ProjectionErrors) extends AnyVal {

    /**
      * Return a search results for the given view on a time window ordered by instant
      *
      * @param view
      *   its identifier
      * @param pagination
      *   the pagination to apply
      * @param timeRange
      *   the time range to restrict on
      * @return
      */
    def search(view: ViewRef, pagination: FromPagination, timeRange: TimeRange): IO[SearchResults[FailedElemData]] = {
      for {
        results <- projectionErrors.list(view.project, view.viewId, pagination, timeRange)
        count   <- projectionErrors.count(view.project, view.viewId, timeRange)
      } yield SearchResults(
        count,
        results.map {
          _.failedElemData
        }
      )
    }.widen[SearchResults[FailedElemData]]

  }

}
