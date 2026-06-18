package ai.senscience.nexus.delta.sourcing.otel

import ai.senscience.nexus.delta.sourcing.stream.ElemChunk
import ai.senscience.nexus.delta.sourcing.stream.config.BatchConfig
import cats.effect.IO
import org.typelevel.otel4s.Attribute

import java.time.Duration

/**
  * Common trace attributes describing a processed batch ([[ElemChunk]]): its size, whether it was full (size-triggered)
  * or partial (time-triggered), the last offset it covered and how stale (lag) its most recent elem was.
  */
object OtelBatchAttributes {

  def apply[A](elements: ElemChunk[A], batch: BatchConfig): IO[Seq[Attribute[?]]] =
    IO.realTimeInstant.map { now =>
      val base: Seq[Attribute[?]] = Seq(
        Attribute("nexus.batch.size", elements.size.toLong),
        Attribute("nexus.batch.trigger", if elements.size == batch.maxElements then "full" else "partial")
      )
      elements.last.fold(base) { last =>
        base ++ Seq(
          Attribute("nexus.batch.offset", last.offset.value),
          Attribute("nexus.batch.lag_seconds", Duration.between(last.instant, now).getSeconds)
        )
      }
    }
}
