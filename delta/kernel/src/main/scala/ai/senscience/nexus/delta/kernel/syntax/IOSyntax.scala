package ai.senscience.nexus.delta.kernel.syntax

import ai.senscience.nexus.delta.kernel.RetryStrategy
import cats.Functor
import cats.effect.IO
import cats.syntax.functor.*

trait IOSyntax {

  extension [A](io: IO[A]) {

    /**
      * Apply the retry strategy on the provided IO
      */
    def retry(retryStrategy: RetryStrategy): IO[A] = RetryStrategy.use(io, retryStrategy)
  }

  extension [A, F[_]: Functor](io: IO[F[A]]) {

    /**
      * Map value of [[F]] wrapped in an [[IO]].
      *
      * @param f
      *   the mapping function
      * @return
      *   a new [[F]] with value being the result of applying [[f]] to the value of old [[F]]
      */
    def mapValue[B](f: A => B): IO[F[B]] = io.map(_.map(f))
  }
}
