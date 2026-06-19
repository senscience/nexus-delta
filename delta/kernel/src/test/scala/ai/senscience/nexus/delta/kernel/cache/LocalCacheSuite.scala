package ai.senscience.nexus.delta.kernel.cache

import ai.senscience.nexus.delta.kernel.config.Configs
import cats.effect.IO
import cats.effect.kernel.Ref
import cats.syntax.all.*
import munit.CatsEffectSuite

import concurrent.duration.DurationInt

class LocalCacheSuite extends CatsEffectSuite {

  test("Test cache config parsing") {
    val config   = "cache-config { enabled = true, max-size = 5 , expire-after = 30s }"
    val expected = CacheConfig(true, 5, 30.seconds)
    val result   = Configs.load[CacheConfig](Configs.parseString(config), "cache-config")
    assertEquals(result, expected)
  }

  test("Test basic operations on cache") {
    val config = CacheConfig(true, 5, 1.hour)
    for {
      cache <- LocalCache[String, Int](config)
      // Populate cache
      _     <- cache.put("5", 5)
      _     <- cache.put("42", 42)
      // Test content
      _     <- cache.containsKey("5").assertEquals(true)
      _     <- cache.get("5").assertEquals(Some(5))
      _     <- cache.get("42").assertEquals(Some(42))
      // Remove and test content
      _     <- cache.remove("5")
      _     <- cache.get("5").assertEquals(None)
      _     <- cache.get("42").assertEquals(Some(42))
      // Update operations
      _     <- cache.getOrElseUpdate("42", IO.pure(0)).assertEquals(42)
      _     <- cache.get("9000").assertEquals(None)
      _     <- cache.getOrElseUpdate("9000", IO.pure(9000)).assertEquals(9000)
      _     <- cache.get("9000").assertEquals(Some(9000))
    } yield ()
  }

  test("getOrElseUpdate computes the value only once under concurrent misses") {
    for {
      cache   <- LocalCache[String, Int](CacheConfig(true, 5, 1.hour))
      counter <- Ref.of[IO, Int](0)
      load     = counter.updateAndGet(_ + 1) <* IO.sleep(50.millis)
      results <- List.fill(20)(cache.getOrElseUpdate("k", load)).parSequence
      _       <- IO(assertEquals(results.toSet, Set(1)))
      _       <- counter.get.assertEquals(1)
    } yield ()
  }

  test("getOrElseAttemptUpdate does not cache a None result") {
    for {
      cache <- LocalCache[String, String](CacheConfig(true, 5, 1.hour))
      _     <- cache.getOrElseAttemptUpdate("k", IO.none).assertEquals(None)
      _     <- cache.get("k").assertEquals(None)
      _     <- cache.getOrElseAttemptUpdate("k", IO.some("v")).assertEquals(Some("v"))
      _     <- cache.get("k").assertEquals(Some("v"))
    } yield ()
  }

}
