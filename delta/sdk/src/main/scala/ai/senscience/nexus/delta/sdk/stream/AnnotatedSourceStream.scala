package ai.senscience.nexus.delta.sdk.stream

import ai.senscience.nexus.delta.sdk.ResourceShifts
import ai.senscience.nexus.delta.sdk.model.source.OriginalSource
import ai.senscience.nexus.delta.sourcing.Scope.Project
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.query.{ElemStreaming, SelectFilter}
import ai.senscience.nexus.delta.sourcing.stream.ElemStream

trait AnnotatedSourceStream {

  /**
    * Allows to generate a non-terminating [[OriginalSource.Annotated]] stream for the given project
    *
    * @param project
    *   the project to stream from
    * @param start
    *   the offset to start with
    */
  def continuous(project: ProjectRef, start: Offset): ElemStream[OriginalSource.Annotated]

}

object AnnotatedSourceStream {
  def apply(elemStreaming: ElemStreaming, shifts: ResourceShifts): AnnotatedSourceStream =
    (project: ProjectRef, start: Offset) =>
      elemStreaming(
        Project(project),
        start,
        SelectFilter.latest,
        shifts.decodeAnnotatedSource(_)(_)
      )
}
