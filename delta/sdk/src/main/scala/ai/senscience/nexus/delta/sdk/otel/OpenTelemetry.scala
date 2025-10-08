package ai.senscience.nexus.delta.sdk.otel

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.sdk.model.Name
import cats.effect.{IO, Resource}
import org.typelevel.otel4s.oteljava.OtelJava

/**
  * Initialize OpenTelemetry with the description config but relying mostly on autoconfiguration
  *
  * @see
  *   https://typelevel.org/otel4s/sdk/configuration.html
  */
sealed trait OpenTelemetry {
  def otelJava: OtelJava[IO]
}

object OpenTelemetry {

  private val logger = Logger[OpenTelemetry.type]

  // Open telemetry is disabled by default
  private def disabled: Boolean =
    sys.props.getOrElse("otel.sdk.disabled", "true").toBooleanOption.getOrElse(true) &&
      sys.env.getOrElse("OTEL_SDK_DISABLED", "true").toBooleanOption.getOrElse(true)

  def apply(name: Name, configure: OtelJava[IO] => IO[Unit]): Resource[IO, OpenTelemetry] = {
    if disabled then {
      Resource.eval(OtelJava.noop[IO]).evalTap { _ =>
        logger.info("OpenTelemetry is disabled.")
      }
    } else {
      sys.props.getOrElseUpdate("otel.service.name", name.value)
      OtelJava.autoConfigured[IO]().evalTap { otel =>
        configure(otel) >> logger.info("OpenTelemetry is enabled.")
      }
    }
  }.map { instance =>
    new OpenTelemetry {
      override def otelJava: OtelJava[IO] = instance
    }
  }
}
