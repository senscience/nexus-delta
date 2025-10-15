package ai.senscience.nexus.delta.otel

import ai.senscience.nexus.delta.config.DescriptionConfig
import ai.senscience.nexus.delta.kernel.Logger
import cats.effect.unsafe.IORuntime
import cats.effect.{IO, Resource}
import cats.syntax.all.*
import io.opentelemetry.instrumentation.logback.appender.v1_0.OpenTelemetryAppender
import io.opentelemetry.instrumentation.runtimemetrics.java8.*
import org.typelevel.otel4s.context.LocalProvider
import org.typelevel.otel4s.instrumentation.ce.IORuntimeMetrics
import org.typelevel.otel4s.metrics.MeterProvider
import org.typelevel.otel4s.oteljava.OtelJava
import org.typelevel.otel4s.oteljava.context.{Context, IOLocalContextStorage}

import scala.jdk.CollectionConverters.*

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

  def apply(description: DescriptionConfig, runtime: IORuntime): Resource[IO, OpenTelemetry] =
    init(description, runtime)
      .map { otel =>
        new OpenTelemetry {
          override def otelJava: OtelJava[IO] = otel
        }
      }

  private def init(description: DescriptionConfig, runtime: IORuntime) = {
    if disabled then {
      Resource
        .eval(OtelJava.noop[IO])
        .evalTap { _ =>
          logger.info("OpenTelemetry is disabled.")
        }
        .flatTap(registerJVMMetrics)
        .flatTap(registerCatsEffectMetrics(_, runtime))
        .evalTap(registerLogback)
    } else {
      given LocalProvider[IO, Context] = IOLocalContextStorage.localProvider[IO]
      sys.props.getOrElseUpdate("otel.service.name", description.name.value)
      OtelJava.autoConfigured[IO]().evalTap { _ =>
        logger.info("OpenTelemetry is enabled.")
      }
    }
  }

  private def registerJVMMetrics(otel: OtelJava[IO]) = {
    val openTelemetry = otel.underlying
    val acquire       = IO.delay {
      List
        .newBuilder[AutoCloseable]
        .addAll(MemoryPools.registerObservers(openTelemetry).asScala)
        .addAll(Classes.registerObservers(openTelemetry).asScala)
        .addAll(Cpu.registerObservers(openTelemetry).asScala)
        .addAll(Threads.registerObservers(openTelemetry).asScala)
        .addAll(GarbageCollector.registerObservers(openTelemetry, true).asScala)
        .result()
    }
    Resource.make(acquire)(r => IO.delay(r.foreach(_.close()))).void
  }

  private def registerCatsEffectMetrics(otel: OtelJava[IO], runtime: IORuntime) = {
    given MeterProvider[IO] = otel.meterProvider
    IORuntimeMetrics
      .register[IO](runtime.metrics, IORuntimeMetrics.Config.default)
  }

  private def registerLogback(otel: OtelJava[IO]) =
    IO.delay { OpenTelemetryAppender.install(otel.underlying) }

}
