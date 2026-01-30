package ai.senscience.nexus.delta.kernel.http

import ai.senscience.nexus.delta.kernel.http.circe.given
import cats.effect.IO
import io.circe.Json
import org.http4s.{EntityDecoder, Response}

object ResponseUtils {

  def decodeBodyAsJson(response: Response[IO]): IO[Json] =
    EntityDecoder[IO, Json]
      .decode(response, strict = false)
      .rethrowT

}
