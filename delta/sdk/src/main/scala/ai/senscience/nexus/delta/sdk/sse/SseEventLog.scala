package ai.senscience.nexus.delta.sdk.sse

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.rdf.syntax.jsonOpsSyntax
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.error.ServiceError.UnknownSseLabel
import ai.senscience.nexus.delta.sdk.marshalling.RdfMarshalling.defaultPrinter
import ai.senscience.nexus.delta.sdk.sse.SseEncoder.SseData
import ai.senscience.nexus.delta.sourcing.event.EventStreaming
import ai.senscience.nexus.delta.sourcing.model.{EntityType, Label, ProjectRef}
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.stream.Elem
import ai.senscience.nexus.delta.sourcing.stream.Elem.{FailedElem, SuccessElem}
import ai.senscience.nexus.delta.sourcing.{MultiDecoder, Scope, Transactors}
import cats.effect.IO
import fs2.Stream
import io.circe.syntax.EncoderOps
import org.apache.pekko.http.scaladsl.model.sse.ServerSentEvent

/**
  * An event log that reads events from a stream and transforms each event to JSON in preparation for consumption by SSE
  * routes
  */
trait SseEventLog {

  /**
    * Get stream of server sent events for the given selector
    *
    * @param selector
    *   to stream only events from a subset of the entity types
    *
    * @param offset
    *   the offset to start from
    */
  def streamBy(selector: Label, offset: Offset): ServerSentEventStream

  /**
    * Get stream of server sent events inside an organization
    *
    * @param org
    *   the organization label
    * @param offset
    *   the offset to start from
    */
  def stream(
      org: Label,
      offset: Offset
  ): IO[ServerSentEventStream]

  /**
    * Get stream of server sent events inside an organization
    *
    * @param selector
    *   to stream only events from a subset of the entity types
    *
    * @param org
    *   the organization label
    * @param offset
    *   the offset to start from
    */
  def streamBy(selector: Label, org: Label, offset: Offset): IO[ServerSentEventStream]

  /**
    * Get stream of server sent events inside an project
    *
    * @param project
    *   the project reference
    * @param offset
    *   the offset to start from
    */
  def stream(
      project: ProjectRef,
      offset: Offset
  ): IO[ServerSentEventStream]

  /**
    * Get stream of server sent events inside an project
    *
    * @param selector
    *   to stream only events from a subset of the entity types
    * @param project
    *   the project reference
    * @param offset
    *   the offset to start from
    */
  def streamBy(selector: Label, project: ProjectRef, offset: Offset): IO[ServerSentEventStream]

  /**
    * Returns SSE selectors related to ScopedEvents
    */
  def selectors: Set[Label]
}

object SseEventLog {

  private val logger = Logger[SseEventLog]

  private[sse] def toServerSentEvent(elem: Elem[SseData])(implicit jo: JsonKeyOrdering): ServerSentEvent = {
    val id = Option.when(elem.offset != Offset.start)(elem.offset.value.toString)
    elem match {
      case e: SuccessElem[SseData] =>
        val data = defaultPrinter.print(e.value.data.asJson.sort)
        new ServerSentEvent(data, Some(e.value.tpe), id)
      case f: FailedElem           =>
        new ServerSentEvent(f.throwable.getMessage, Some("Error"), id)
      case _: Elem.DroppedElem     =>
        new ServerSentEvent("", Some("Deleted"), id)
    }
  }

  def apply(
      sseEncoders: Set[SseEncoder[?]],
      fetchOrg: Label => IO[Unit],
      fetchProject: ProjectRef => IO[Unit],
      config: SseConfig,
      xas: Transactors
  )(implicit jo: JsonKeyOrdering): IO[SseEventLog] =
    IO.pure {
      new SseEventLog {
        implicit private val multiDecoder: MultiDecoder[SseData] =
          MultiDecoder(sseEncoders.map { encoder => encoder.entityType -> encoder.toSse }.toMap)

        private val entityTypesBySelector: Map[Label, List[EntityType]] = sseEncoders
          .flatMap { encoder => encoder.selectors.map(_ -> encoder.entityType) }
          .groupMap(_._1)(_._2)
          .map { case (k, v) => k -> v.toList }

        override val selectors: Set[Label] = sseEncoders.flatMap(_.selectors)

        private def stream(scope: Scope, selector: Option[Label], offset: Offset): ServerSentEventStream = {
          Stream
            .fromEither[IO](
              selector
                .map { l =>
                  entityTypesBySelector.get(l).toRight(UnknownSseLabel(l))
                }
                .getOrElse(Right(List.empty))
            )
            .flatMap { entityTypes =>
              EventStreaming
                .fetchScoped(
                  scope,
                  entityTypes,
                  offset,
                  config.query,
                  xas
                )
                .map(toServerSentEvent)
            }
        }

        override def streamBy(selector: Label, offset: Offset): ServerSentEventStream =
          stream(Scope.root, Some(selector), offset)

        override def stream(org: Label, offset: Offset): IO[ServerSentEventStream] =
          fetchOrg(org).as(stream(Scope.Org(org), None, offset))

        override def streamBy(selector: Label, org: Label, offset: Offset): IO[ServerSentEventStream] =
          fetchOrg(org).as(stream(Scope.Org(org), Some(selector), offset))

        override def stream(project: ProjectRef, offset: Offset): IO[ServerSentEventStream] =
          fetchProject(project).as(stream(Scope.Project(project), None, offset))

        override def streamBy(selector: Label, project: ProjectRef, offset: Offset): IO[ServerSentEventStream] =
          fetchProject(project).as(stream(Scope.Project(project), Some(selector), offset))
      }
    }.flatTap { sseLog =>
      logger.info(s"SseLog is configured with selectors: ${sseLog.selectors.mkString("'", "','", "'")}")
    }

}
