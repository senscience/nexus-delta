package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.stream.Operation.Sink
import cats.data.NonEmptyChain
import cats.effect.{IO, Ref}
import fs2.{Pull, Stream}
import fs2.concurrent.SignallingRef

import scala.concurrent.duration.DurationInt

/**
  * A projection that has been successfully compiled and is ready to be run.
  *
  * @param metadata
  *   the metadata of the projection
  * @param streamF
  *   a fn that produces a stream given a starting offset, a status reference and a stop signal
  */
final case class CompiledProjection private (
    metadata: ProjectionMetadata,
    executionStrategy: ExecutionStrategy,
    streamF: Offset => Ref[IO, ExecutionStatus] => SignallingRef[IO, Boolean] => ElemStream[Unit]
)

object CompiledProjection {

  /**
    * Creates a projection from a provided task
    */
  def fromTask(
      metadata: ProjectionMetadata,
      executionStrategy: ExecutionStrategy,
      task: IO[Unit]
  )(using backpressure: ProjectionBackpressure): CompiledProjection =
    fromStream(metadata, executionStrategy, _ => Stream.eval(task).drain)

  /**
    * Creates a projection from a provided stream
    */
  def fromStream(
      metadata: ProjectionMetadata,
      executionStrategy: ExecutionStrategy,
      stream: Offset => ElemStream[Unit]
  )(using backpressure: ProjectionBackpressure): CompiledProjection =
    CompiledProjection(
      metadata,
      executionStrategy,
      offset => _ => _ => applyBackpressure(metadata, stream(offset))
    )

  /**
    * Attempts to compile the projection with just a source and a sink.
    */
  def compile(
      metadata: ProjectionMetadata,
      executionStrategy: ExecutionStrategy,
      source: Source,
      sink: Sink
  )(using backpressure: ProjectionBackpressure): Either[ProjectionErr, CompiledProjection] =
    source.through(sink).map { p =>
      CompiledProjection(
        metadata,
        executionStrategy,
        offset => _ => _ => applyBackpressure(metadata, p.apply(offset).map(_.void))
      )
    }

  /**
    * Attempts to compile the projection definition that can be later managed.
    */
  def compile(
      metadata: ProjectionMetadata,
      executionStrategy: ExecutionStrategy,
      source: Source,
      chain: NonEmptyChain[Operation],
      sink: Sink
  )(using backpressure: ProjectionBackpressure): Either[ProjectionErr, CompiledProjection] = {
    for {
      operations <- Operation.merge(chain ++ NonEmptyChain.one(sink))
      result     <- source.through(operations)
    } yield CompiledProjection(
      metadata,
      executionStrategy,
      offset => _ => _ => applyBackpressure(metadata, result.apply(offset).map(_.void))
    )
  }

  private def applyBackpressure[A](metadata: ProjectionMetadata, stream: ElemStream[A])(using
      backpressure: ProjectionBackpressure
  ) = {
    def go(s: ElemStream[A]): Pull[IO, Elem[A], Unit] = {
      Pull.eval(backpressure.exhausted).flatMap {
        case true =>
          Pull.sleep(100.millis) >> go(s)
        case false =>
          s.pull.uncons.flatMap {
            case Some((head, tail)) =>
              Pull.bracketCase(
                Pull.eval(backpressure.acquire(metadata, head.size)),
                _ => Pull.output(head),
                (_, _) => Pull.eval(backpressure.release(metadata, head.size))
              ) >> go(tail)
            case None => Pull.done
          }
      }
    }
    go(stream).stream
  }
}
