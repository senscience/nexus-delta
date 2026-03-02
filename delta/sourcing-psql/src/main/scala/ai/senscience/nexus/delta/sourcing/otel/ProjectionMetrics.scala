package ai.senscience.nexus.delta.sourcing.otel

import ai.senscience.nexus.delta.sourcing.stream.{ProjectionMetadata, ProjectionProgress}
import cats.effect.IO
import org.typelevel.otel4s.metrics.MeterProvider
import org.typelevel.otel4s.{Attribute, AttributeKey, Attributes}

trait ProjectionMetrics {

  def recordProgress(metadata: ProjectionMetadata, progress: ProjectionProgress): IO[Unit]

}

object ProjectionMetrics {

  private val moduleKey = AttributeKey[String]("nexus.module")

  case object Disabled extends ProjectionMetrics {
    override def recordProgress(metadata: ProjectionMetadata, progress: ProjectionProgress): IO[Unit] = IO.unit
  }

  final class Enabled(metrics: ProjectionMetricsCollection) extends ProjectionMetrics {

    override def recordProgress(metadata: ProjectionMetadata, progress: ProjectionProgress): IO[Unit] = {
      val attributes = makeAttributes(metadata)
      metrics.processed
        .record(progress.processed, attributes) >>
        metrics.discarded
          .record(progress.discarded, attributes) >>
        metrics.failed
          .record(progress.failed, attributes)
    }

    private def makeAttributes(metadata: ProjectionMetadata) = {
      val attributes = Attributes.newBuilder
      attributes += Attribute(moduleKey, metadata.module)
      attributes.result()
    }
  }

  def apply(version: String)(using MeterProvider[IO]): IO[ProjectionMetrics] =
    MeterProvider[IO]
      .meter("ai.senscience.nexus.delta.projections")
      .withVersion(version)
      .get
      .flatMap { meter =>
        meter.meta.isEnabled.flatMap {
          case false => IO.pure(Disabled)
          case true  => ProjectionMetricsCollection(meter).map(Enabled(_))
        }
      }

}
