package ai.senscience.nexus.delta.kernel.syntax

import cats.effect.IO
import org.http4s.Response

trait Http4sResponseSyntax {
  extension (response: Response[IO]) {
    def bodyAsString: IO[String] = response.body.compile.to(Array).map(new String(_))
  }
}
