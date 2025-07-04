package ai.senscience.nexus.delta.sdk.stream

import ai.senscience.nexus.delta.sdk.ResourceShifts
import ai.senscience.nexus.delta.sourcing.Scope
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.query.{ElemStreaming, SelectFilter}
import ai.senscience.nexus.delta.sourcing.state.GraphResource
import ai.senscience.nexus.delta.sourcing.stream.{ElemStream, RemainingElems}
import cats.effect.IO
import fs2.Stream

trait GraphResourceStream {

  /**
    * Allows to generate a non-terminating [[GraphResource]] stream for the given project for the given tag
    * @param project
    *   the project to stream from
    * @param selectFilter
    *   what to filter for
    * @param start
    *   the offset to start with
    */
  def continuous(project: ProjectRef, selectFilter: SelectFilter, start: Offset): ElemStream[GraphResource]

  /**
    * Allows to generate a [[GraphResource]] stream for the given project for the given tag
    *
    * This stream terminates when reaching all elements have been returned
    *
    * @param project
    *   the project to stream from
    * @param selectFilter
    *   what to filter for
    * @param start
    *   the offset to start with
    */
  def currents(project: ProjectRef, selectFilter: SelectFilter, start: Offset): ElemStream[GraphResource]

  /**
    * Get information about the remaining elements to stream
    * @param project
    *   the project to stream from
    * @param selectFilter
    *   what to filter for
    * @param start
    *   the offset to start with
    */
  def remaining(project: ProjectRef, selectFilter: SelectFilter, start: Offset): IO[Option[RemainingElems]]
}

object GraphResourceStream {

  /**
    * Creates an empty graph resource stream
    */
  val empty: GraphResourceStream = new GraphResourceStream {
    override def continuous(project: ProjectRef, selectFilter: SelectFilter, start: Offset): ElemStream[GraphResource] =
      Stream.never[IO]
    override def currents(project: ProjectRef, selectFilter: SelectFilter, start: Offset): ElemStream[GraphResource]   =
      Stream.empty
    override def remaining(
        project: ProjectRef,
        selectFilter: SelectFilter,
        start: Offset
    ): IO[Option[RemainingElems]] = IO.none
  }

  /**
    * Create a graph resource stream
    */
  def apply(elemStreaming: ElemStreaming, shifts: ResourceShifts): GraphResourceStream =
    new GraphResourceStream {

      val stopping: ElemStreaming = elemStreaming.stopping

      override def continuous(
          project: ProjectRef,
          selectFilter: SelectFilter,
          start: Offset
      ): ElemStream[GraphResource] =
        elemStreaming(Scope(project), start, selectFilter, shifts.decodeGraphResource(_, _))

      override def currents(
          project: ProjectRef,
          selectFilter: SelectFilter,
          start: Offset
      ): ElemStream[GraphResource] =
        stopping(Scope(project), start, selectFilter, shifts.decodeGraphResource(_, _))

      override def remaining(
          project: ProjectRef,
          selectFilter: SelectFilter,
          start: Offset
      ): IO[Option[RemainingElems]] =
        elemStreaming.remaining(Scope(project), selectFilter, start)
    }

  /**
    * Constructs a GraphResourceStream from an existing stream without assuring the termination of the operations.
    */
  def unsafeFromStream(stream: ElemStream[GraphResource]): GraphResourceStream =
    new GraphResourceStream {
      override def continuous(
          project: ProjectRef,
          selectFilter: SelectFilter,
          start: Offset
      ): ElemStream[GraphResource] = stream
      override def currents(project: ProjectRef, selectFilter: SelectFilter, start: Offset): ElemStream[GraphResource] =
        stream
      override def remaining(
          project: ProjectRef,
          selectFilter: SelectFilter,
          start: Offset
      ): IO[Option[RemainingElems]] = IO.none
    }

}
