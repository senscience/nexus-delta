package ai.senscience.nexus.delta.kernel.cache

import ai.senscience.nexus.delta.kernel.config.Configs
import cats.effect.IO
import munit.CatsEffectSuite

import concurrent.duration.DurationInt

class LocalCacheSuite extends CatsEffectSuite {

  test("Test cache config parsing") {
    val config   = "cache-config { max-size = 5 , expire-after = 30s }"
    val expected = CacheConfig(5, 30.seconds)
    val result   = Configs.load[CacheConfig](Configs.parseString(config), "cache-config")
    assertEquals(result, expected)
  }

  test("Test basic operations on cache") {
    val config = CacheConfig(5, 1.hour)
    for {
      cache <- LocalCache[String, Int](config)
      // Populate cache
      _     <- cache.put("5", 5)
      _     <- cache.put("42", 42)
      // Test content
      _     <- cache.containsKey("5").assertEquals(true)
      _     <- cache.get("5").assertEquals(Some(5))
      _     <- cache.values.map(_.sorted).assertEquals(Vector(5, 42))
      // Remove and test content
      _     <- cache.remove("5")
      _     <- cache.get("5").assertEquals(None)
      _     <- cache.values.map(_.sorted).assertEquals(Vector(42))
      // Update operations
      _     <- cache.getOrElseUpdate("42", IO.pure(0)).assertEquals(42)
      _     <- cache.get("9000").assertEquals(None)
      _     <- cache.getOrElseUpdate("9000", IO.pure(9000)).assertEquals(9000)
      _     <- cache.values.map(_.sorted).assertEquals(Vector(42, 9000))
    } yield ()
  }

}
