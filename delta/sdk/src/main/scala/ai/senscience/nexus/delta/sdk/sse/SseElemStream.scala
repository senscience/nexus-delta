package ai.senscience.nexus.delta.sdk.sse

import ai.senscience.nexus.delta.sdk.marshalling.RdfMarshalling.defaultPrinter
import ai.senscience.nexus.delta.sourcing.Scope
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.query.{ElemStreaming, SelectFilter}
import ai.senscience.nexus.delta.sourcing.stream.Elem.{DroppedElem, FailedElem, SuccessElem}
import ai.senscience.nexus.delta.sourcing.stream.{Elem, RemainingElems}
import akka.http.scaladsl.model.sse.ServerSentEvent
import cats.effect.IO
import io.circe.syntax.EncoderOps

trait SseElemStream {

  /**
    * Allows to generate a non-terminating [[ServerSentEvent]] stream for the given project for the given tag
    * @param project
    *   the project to stream from
    * @param tag
    *   the tag to retain
    * @param start
    *   the offset to start with
    */
  def continuous(project: ProjectRef, selectFilter: SelectFilter, start: Offset): ServerSentEventStream

  /**
    * Allows to generate a [[ServerSentEvent]] stream for the given project for the given tag
    *
    * This stream terminates when reaching all elements have been returned
    *
    * @param project
    *   the project to stream from
    * @param tag
    *   the tag to retain
    * @param start
    *   the offset to start with
    */
  def currents(project: ProjectRef, selectFilter: SelectFilter, start: Offset): ServerSentEventStream

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

object SseElemStream {

  /**
    * Create a [[SseElemStream]]
    */
  def apply(elemStreaming: ElemStreaming): SseElemStream = new SseElemStream {

    val stopping = elemStreaming.stopping

    override def continuous(project: ProjectRef, selectFilter: SelectFilter, start: Offset): ServerSentEventStream =
      elemStreaming(Scope(project), start, selectFilter).map(toServerSentEvent)

    override def currents(project: ProjectRef, selectFilter: SelectFilter, start: Offset): ServerSentEventStream =
      stopping(Scope(project), start, selectFilter).map(toServerSentEvent)

    override def remaining(project: ProjectRef, selectFilter: SelectFilter, start: Offset): IO[Option[RemainingElems]] =
      elemStreaming.remaining(Scope(project), selectFilter, start)
  }

  private[sse] def toServerSentEvent(elem: Elem[Unit]): ServerSentEvent = {
    val tpe = elem match {
      case _: SuccessElem[Unit] => "Success"
      case _: DroppedElem       => "Dropped"
      case _: FailedElem        => "Failed"
    }
    ServerSentEvent(defaultPrinter.print(elem.asJson), tpe, elem.offset.value.toString)
  }
}
