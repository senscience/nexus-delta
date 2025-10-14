package ai.senscience.nexus.delta.wiring

import ai.senscience.nexus.delta.config.BuildInfo
import ai.senscience.nexus.delta.config.DescriptionConfig
import ai.senscience.nexus.delta.otel.OpenTelemetry
import ai.senscience.nexus.delta.sdk.otel.OtelMetricsClient
import ai.senscience.nexus.delta.sdk.wiring.NexusModuleDef
import cats.effect.IO
import cats.effect.unsafe.IORuntime
import org.typelevel.otel4s.oteljava.OtelJava

final class OtelModule(runtime: IORuntime) extends NexusModuleDef {

  make[OpenTelemetry].fromResource { (description: DescriptionConfig) =>
    OpenTelemetry(description, runtime)
  }

  make[OtelJava[IO]].from { (otel: OpenTelemetry) => otel.otelJava }

  make[OtelMetricsClient].fromEffect { (otel: OtelJava[IO]) =>
    OtelMetricsClient(BuildInfo.version)(using otel.meterProvider)
  }

}
