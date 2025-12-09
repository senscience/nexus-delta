package ai.senscience.nexus.delta.sdk.stream

import ai.senscience.nexus.delta.sdk.indexing.MainDocument
import ai.senscience.nexus.delta.sdk.indexing.MainDocumentEncoder
import ai.senscience.nexus.delta.sourcing.Scope.Project
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.query.{ElemStreaming, SelectFilter}
import ai.senscience.nexus.delta.sourcing.stream.ElemStream

trait MainDocumentStream {

  /**
    * Allows to generate a non-terminating [[MainDocument]] stream for the given project
    *
    * @param project
    *   the project to stream from
    * @param start
    *   the offset to start with
    */
  def continuous(project: ProjectRef, start: Offset): ElemStream[MainDocument]

}

object MainDocumentStream {
  def apply(elemStreaming: ElemStreaming, mainDocumentEncoder: MainDocumentEncoder.Aggregate): MainDocumentStream =

    (project: ProjectRef, start: Offset) =>
      elemStreaming(
        Project(project),
        start,
        SelectFilter.latest,
        mainDocumentEncoder.fromJson(_)(_)
      )
}
