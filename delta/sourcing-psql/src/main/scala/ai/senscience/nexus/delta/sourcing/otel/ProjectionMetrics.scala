package ai.senscience.nexus.delta.sourcing.otel

import ai.senscience.nexus.delta.sourcing.otel.ProjectionMetrics.TerminationOutcome
import ai.senscience.nexus.delta.sourcing.stream.{ProjectionActivation, ProjectionMetadata, ProjectionProgress}
import cats.effect.{IO, Resource}
import cats.syntax.all.*
import org.typelevel.otel4s.metrics.{Measurement, Meter, MeterProvider}
import org.typelevel.otel4s.{Attribute, AttributeKey, Attributes}

trait ProjectionMetrics {

  /** Records a per-batch progress delta (newly processed/discarded/failed elems) for a projection. */
  def recordProgress(metadata: ProjectionMetadata, delta: ProjectionProgress): IO[Unit]

  /** Records that a projection terminated, either by completing (incl. passivation) or by failing. */
  def recordTermination(metadata: ProjectionMetadata, outcome: TerminationOutcome): IO[Unit]

  /** Records that an activation was published, which triggers the matching projections to resume. */
  def recordActivation(activation: ProjectionActivation): IO[Unit]

  /**
    * Registers an observable gauge reporting, per module, the number of projections currently supervised. `running` is
    * evaluated on each metric collection cycle.
    */
  def monitorRunning(running: IO[List[ProjectionMetadata]]): Resource[IO, Unit]

}

object ProjectionMetrics {

  private val moduleKey  = AttributeKey[String]("nexus.module")
  private val outcomeKey = AttributeKey[String]("outcome")
  private val kindKey    = AttributeKey[String]("kind")

  /** Why a projection's stream terminated. For continuous indexing projections, `Completed` is a passivation. */
  enum TerminationOutcome(val label: String) {
    case Completed extends TerminationOutcome("completed")
    case Failed    extends TerminationOutcome("failed")
  }

  case object Disabled extends ProjectionMetrics {
    override def recordProgress(metadata: ProjectionMetadata, delta: ProjectionProgress): IO[Unit]      = IO.unit
    override def recordTermination(metadata: ProjectionMetadata, outcome: TerminationOutcome): IO[Unit] = IO.unit
    override def recordActivation(activation: ProjectionActivation): IO[Unit]                           = IO.unit
    override def monitorRunning(running: IO[List[ProjectionMetadata]]): Resource[IO, Unit]              = Resource.unit
  }

  final class Enabled(meter: Meter[IO], metrics: ProjectionMetricsCollection) extends ProjectionMetrics {

    override def recordProgress(metadata: ProjectionMetadata, delta: ProjectionProgress): IO[Unit] = {
      val attributes = List(Attribute(moduleKey, metadata.module))
      metrics.processed.add(delta.processed, attributes) >>
        metrics.discarded.add(delta.discarded, attributes) >>
        metrics.failed.add(delta.failed, attributes)
    }

    override def recordTermination(metadata: ProjectionMetadata, outcome: TerminationOutcome): IO[Unit] =
      metrics.terminated.add(1L, List(Attribute(moduleKey, metadata.module), Attribute(outcomeKey, outcome.label)))

    override def recordActivation(activation: ProjectionActivation): IO[Unit] = {
      val kind = activation match {
        case _: ProjectionActivation.ForProject    => "project"
        case _: ProjectionActivation.ForProjection => "projection"
      }
      metrics.activations.add(1L, List(Attribute(kindKey, kind)))
    }

    override def monitorRunning(running: IO[List[ProjectionMetadata]]): Resource[IO, Unit] =
      meter
        .observableGauge[Long]("nexus.indexing.projections.running")
        .withUnit("{projection}")
        .withDescription("Number of projections currently supervised, by module.")
        .create(
          running.map { metadatas =>
            metadatas.groupBy(_.module).toList.map { case (module, ms) =>
              Measurement(ms.size.toLong, Attributes(Attribute(moduleKey, module)))
            }
          }
        )
        .void
  }

  def apply(version: String)(using MeterProvider[IO]): IO[ProjectionMetrics] =
    MeterProvider[IO]
      .meter("ai.senscience.nexus.delta.projections")
      .withVersion(version)
      .get
      .flatMap { meter =>
        meter.meta.isEnabled.flatMap {
          case false => IO.pure(Disabled)
          case true  => ProjectionMetricsCollection(meter).map(new Enabled(meter, _))
        }
      }

}
