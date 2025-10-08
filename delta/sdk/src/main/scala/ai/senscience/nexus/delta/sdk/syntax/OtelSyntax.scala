package ai.senscience.nexus.delta.sdk.syntax

import cats.effect.IO
import org.typelevel.otel4s.Attribute
import org.typelevel.otel4s.trace.Tracer

trait OtelSyntax {

  extension [A](io: IO[A])(using Tracer[IO]) {
    def surround(name: String, attributes: Attribute[?]*): IO[A] =
      Tracer[IO].span(name, attributes).surround(io)
  }

}
