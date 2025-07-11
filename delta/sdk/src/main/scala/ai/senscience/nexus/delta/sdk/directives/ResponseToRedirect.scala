package ai.senscience.nexus.delta.sdk.directives

import akka.http.scaladsl.model.StatusCodes.Redirection
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.server.Directives.*
import akka.http.scaladsl.server.Route
import cats.effect.IO
import cats.effect.unsafe.implicits.*
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
          redirect(toAkka(uri), redirection)
        }
    }

  private def toAkka(uri: Http4sUri): Uri = Uri(uri.toString())
}
