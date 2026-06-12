package ai.senscience.nexus.testkit

import cats.effect.{IO, Ref}

/**
  * A small counter keyed by `K`, useful as a deterministic signal that a callback was invoked.
  */
final class InvocationCounter[K] private (ref: Ref[IO, Map[K, Int]]) {

  /** Records one invocation for the given key. */
  def increment(key: K): IO[Unit] =
    ref.update(m => m.updated(key, m.getOrElse(key, 0) + 1))

  /** Current invocation count for the given key (0 if none). */
  def count(key: K): IO[Int] =
    ref.get.map(_.getOrElse(key, 0))
}

object InvocationCounter {

  def apply[K](): InvocationCounter[K] = new InvocationCounter(Ref.unsafe[IO, Map[K, Int]](Map.empty))
}
