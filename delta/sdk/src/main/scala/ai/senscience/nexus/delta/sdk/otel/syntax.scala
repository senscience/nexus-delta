package ai.senscience.nexus.delta.sdk.otel

import cats.effect.IO
import org.http4s.Status
import org.http4s.client.Client
import org.typelevel.otel4s.trace.Tracer

object syntax {

  extension (client: Client[IO]) {

    /**
      * Returns a client that traces each request under `spanDef`. Only successful responses are expected: any 4xx/5xx
      * marks the span as errored.
      */
    def traced(spanDef: SpanDef)(using Tracer[IO]): Client[IO] =
      OtelTracingClient(client, spanDef)

    /**
      * Like [[traced]], but `404 Not Found` and `409 Conflict` are treated as normal outcomes (e.g. existence,
      * delete-if-exists or create-if-absent calls where the caller handles those statuses) and do not error the span.
      */
    def tracedRecover(spanDef: SpanDef)(using Tracer[IO]): Client[IO] =
      OtelTracingClient(client, spanDef, Status.NotFound, Status.Conflict)
  }
}
