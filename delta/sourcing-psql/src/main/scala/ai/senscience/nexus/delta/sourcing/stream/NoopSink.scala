package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.sourcing.stream.Operation.Sink
import ai.senscience.nexus.delta.sourcing.stream.config.BatchConfig
import cats.effect.IO
import shapeless.Typeable

import scala.concurrent.duration.*

final class NoopSink[A: Typeable] extends Sink {

  override type In = A

  override def inType: Typeable[A] = Typeable[A]

  override def batchConfig: BatchConfig = BatchConfig(1, 10.millis)

  override def apply(elements: ElemChunk[A]): IO[ElemChunk[Unit]] =
    IO.pure(elements.map(_.void))
}
