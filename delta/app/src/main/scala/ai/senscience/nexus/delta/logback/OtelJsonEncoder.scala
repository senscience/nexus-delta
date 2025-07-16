package ai.senscience.nexus.delta.logback

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.spi.{ILoggingEvent, ThrowableProxyUtil}
import ch.qos.logback.core.encoder.EncoderBase
import io.circe.syntax.KeyOps
import io.circe.{Json, Printer}
import io.opentelemetry.api.logs.Severity
import io.opentelemetry.semconv.ExceptionAttributes.*

import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters.*

class OtelJsonEncoder extends EncoderBase[ILoggingEvent] {

  private val printer: Printer = Printer(dropNullValues = true, indent = "")

  override def headerBytes(): Array[Byte] = Array.emptyByteArray

  private val exceptionType       = EXCEPTION_TYPE.getKey
  private val exceptionMessage    = EXCEPTION_MESSAGE.getKey
  private val exceptionStacktrace = EXCEPTION_STACKTRACE.getKey

  private val serviceName =
    sys.props.get("otel.service.name").orElse(sys.env.get("OTEL_SERVICE_NAME")).getOrElse("delta")

  override def encode(event: ILoggingEvent): Array[Byte] = {
    val exceptionFields =
      Option(event.getThrowableProxy)
        .map { e =>
          Map(
            exceptionType       := e.getClassName,
            exceptionMessage    := e.getMessage,
            exceptionStacktrace := ThrowableProxyUtil.asString(e)
          )
        }
        .getOrElse(Map.empty)

    val mdcFields = event.getMDCPropertyMap.asScala.map { case (key, value) =>
      key := value
    }

    val attributes = Json.fromFields(
      Map("logger.name" := event.getLoggerName) ++ exceptionFields ++ mdcFields
    )

    val resource = Json.obj(
      "service.name" := serviceName,
      "attributes"   := Json.obj()
    )

    val timestamp = timestampInNanos(event.getTimeStamp, event.getNanoseconds)

    if (event.getNanoseconds == -1) {
      TimeUnit.MILLISECONDS.toNanos(event.getTimeStamp)
    } else {
      TimeUnit.MILLISECONDS.toNanos(event.getTimeStamp) + event.getNanoseconds
    }

    TimeUnit.MILLISECONDS.toNanos(event.getTimeStamp)
    val json = Json.fromFields(
      Map(
        "severityText"   := event.getLevel.toString,
        "severityNumber" := severity(event.getLevel).getSeverityNumber,
        "timestamp"      := timestamp,
        "body"           := event.getFormattedMessage,
        "attributes"     := attributes,
        "resource"       := resource
      )
    )
    (printer.print(json) + '\n').getBytes
  }

  private def timestampInNanos(timestamp: Long, nanos: Int) =
    TimeUnit.MILLISECONDS.toNanos(timestamp) + nanos.max(0)

  private def severity(level: Level) =
    level.toInt match {
      case Level.ALL_INT | Level.TRACE_INT => Severity.TRACE
      case Level.DEBUG_INT                 => Severity.DEBUG
      case Level.INFO_INT                  => Severity.INFO
      case Level.WARN_INT                  => Severity.WARN
      case Level.ERROR_INT                 => Severity.ERROR
      case _                               => Severity.UNDEFINED_SEVERITY_NUMBER
    }

  override def footerBytes(): Array[Byte] = Array.emptyByteArray
}
