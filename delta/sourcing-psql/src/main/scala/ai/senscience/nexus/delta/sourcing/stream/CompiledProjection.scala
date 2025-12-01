package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.stream.Operation.Sink
import cats.data.NonEmptyChain
import cats.effect.IO
import fs2.Stream

/**
  * A projection that has been successfully compiled and is ready to be run.
  *
  * @param metadata
  *   the metadata of the projection
  * @param streamF
  *   a fn that produces a stream given a starting offset
  */
final case class CompiledProjection private (
    metadata: ProjectionMetadata,
    executionStrategy: ExecutionStrategy,
    streamF: Offset => ElemStream[Unit]
)

object CompiledProjection {

  /**
    * Creates a projection from a provided task
    */
  def fromTask(metadata: ProjectionMetadata, executionStrategy: ExecutionStrategy, io: IO[Unit]): CompiledProjection =
    fromStream(metadata, executionStrategy, _ => Stream.eval(io).drain)

  /**
    * Creates a projection from a provided stream
    */
  def fromStream(
      metadata: ProjectionMetadata,
      executionStrategy: ExecutionStrategy,
      stream: Offset => ElemStream[Unit]
  ): CompiledProjection =
    CompiledProjection(
      metadata,
      executionStrategy,
      offset => stream(offset)
    )

  /**
    * Attempts to compile the projection with just a source and a sink.
    */
  def compile(
      metadata: ProjectionMetadata,
      executionStrategy: ExecutionStrategy,
      source: Source,
      sink: Sink
  ): Either[ProjectionErr, CompiledProjection] =
    source.through(sink).map { p =>
      CompiledProjection(
        metadata,
        executionStrategy,
        offset => p.apply(offset).map(_.void)
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
  ): Either[ProjectionErr, CompiledProjection] =
    for {
      operations <- Operation.merge(chain ++ NonEmptyChain.one(sink))
      result     <- source.through(operations)
    } yield CompiledProjection(
      metadata,
      executionStrategy,
      offset => result.apply(offset).map(_.void)
    )
}
