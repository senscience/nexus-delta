package ai.senscience.nexus.delta.kernel.cache

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.benmanes.caffeine.cache.{AsyncCache, Caffeine}

import java.util.concurrent.Executor
import scala.concurrent.duration.*
import scala.jdk.DurationConverters.*

/**
  * An arbitrary key value store.
  *
  * @tparam K
  *   the key type
  * @tparam V
  *   the value type
  */
trait LocalCache[K, V] {

  /**
    * Adds the (key, value) to the store, replacing the current value if the key already exists.
    *
    * @param key
    *   the key under which the value is stored
    * @param value
    *   the value stored
    */
  def put(key: K, value: V): IO[Unit]

  /**
    * Deletes a key from the store.
    *
    * @param key
    *   the key to be deleted from the store
    */
  def remove(key: K): IO[Unit]

  /**
    * @param key
    *   the key
    * @return
    *   an optional value for the provided key
    */
  def get(key: K): IO[Option[V]]

  /**
    * Fetch the value for the given key and if not, compute the new value, insert it in the store and return it.
    * Concurrent lookups for the same missing key are de-duplicated: `op` runs at most once.
    * @param key
    *   the key
    * @param op
    *   the computation yielding the value to associate with `key`, if `key` is previously unbound.
    */
  def getOrElseUpdate(key: K, op: => IO[V]): IO[V]

  /**
    * Fetch the value for the given key and if not, compute the new value, insert it in the store if defined and return
    * it.
    * @param key
    *   the key
    * @param op
    *   the computation yielding the value to associate with `key`, if `key` is previously unbound.
    */
  def getOrElseAttemptUpdate(key: K, op: => IO[Option[V]]): IO[Option[V]] =
    getOrElseUpdate(key, op.map(_.getOrElse(null.asInstanceOf[V]))).map(Option(_))

  /**
    * Tests whether the cache contains the given key.
    * @param key
    *   the key to be tested
    */
  def containsKey(key: K): IO[Boolean] = get(key).map(_.isDefined)

}

object LocalCache {

  def noop[K, V]: LocalCache[K, V] = new NoCache[K, V]

  /**
    * Constructs a local key-value store
    */
  final def apply[K, V](): IO[LocalCache[K, V]] =
    IO.delay {
      val cache: AsyncCache[K, V] =
        Caffeine.newBuilder().buildAsync[K, V]()
      new LocalCacheImpl(cache)
    }

  /**
    * Constructs a local key-value store
    *
    * @param config
    *   the cache configuration
    */
  final def apply[K, V](config: CacheConfig): IO[LocalCache[K, V]] =
    if config.enabled then apply(config.maxSize.toLong, config.expireAfter)
    else IO.pure(noop[K, V])

  /**
    * Constructs a local key-value store
    * @param maxSize
    *   the max number of entries
    * @param expireAfterWrite
    *   Entries will be removed one the given duration has elapsed after the entry's creation or the most recent
    *   replacement of its value.
    */
  final def apply[K, V](maxSize: Long, expireAfterWrite: FiniteDuration = 1.hour): IO[LocalCache[K, V]] =
    IO.delay {
      val cache: AsyncCache[K, V] =
        Caffeine
          .newBuilder()
          .expireAfterWrite(expireAfterWrite.toJava)
          .maximumSize(maxSize)
          .buildAsync[K, V]()
      new LocalCacheImpl(cache)
    }

  private class NoCache[K, V] extends LocalCache[K, V] {
    override def put(key: K, value: V): IO[Unit] = IO.unit

    override def get(key: K): IO[Option[V]] = IO.none

    override def remove(key: K): IO[Unit] = IO.unit

    override def getOrElseUpdate(key: K, op: => IO[V]): IO[V] = op
  }

  private class LocalCacheImpl[K, V](cache: AsyncCache[K, V]) extends LocalCache[K, V] {

    // Synchronous view over the async cache: it only exposes entries whose load has completed
    // successfully (in-flight or failed loads are treated as absent), and never blocks.
    private val sync = cache.synchronous()

    override def put(key: K, value: V): IO[Unit] = IO.delay(sync.put(key, value))

    override def get(key: K): IO[Option[V]] = IO.delay(Option(sync.getIfPresent(key)))

    override def remove(key: K): IO[Unit] = IO.delay(sync.invalidate(key))

    // Atomic, single-flight load: the first caller installs the in-flight `CompletableFuture` and any
    // concurrent caller of the same key awaits that same future instead of recomputing `op`. Neither a failed
    // load nor one completing with `null` (the `None` sentinel used by getOrElseAttemptUpdate) is cached:
    // Caffeine drops the entry in both cases.
    override def getOrElseUpdate(key: K, op: => IO[V]): IO[V] =
      IO.fromCompletableFuture {
        IO.delay {
          cache.get(key, (_: K, _: Executor) => op.unsafeToCompletableFuture())
        }
      }
  }
}
