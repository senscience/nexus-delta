package ai.senscience.nexus.delta.sdk.directives

import cats.effect.IO
import cats.effect.unsafe.implicits.*
import org.apache.pekko.http.scaladsl.model.StatusCodes.Redirection
import org.apache.pekko.http.scaladsl.model.Uri
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route
import org.http4s.Uri as Http4sUri

/**
  * Redirection response magnet.
  */
sealed trait ResponseToRedirect {
  def apply(redirection: Redirection): Route
}

object ResponseToRedirect {

  implicit def ioRedirect(io: IO[Http4sUri]): ResponseToRedirect =
    new ResponseToRedirect {
      override def apply(redirection: Redirection): Route =
        onSuccess(io.unsafeToFuture()) { uri =>
          redirect(toPekko(uri), redirection)
        }
    }

  private def toPekko(uri: Http4sUri): Uri = Uri(uri.toString())
}
