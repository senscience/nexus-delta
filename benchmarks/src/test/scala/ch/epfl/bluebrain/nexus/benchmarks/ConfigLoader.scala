package ch.epfl.bluebrain.nexus.benchmarks

import ai.senscience.nexus.delta.kernel.config.Configs
import cats.effect.unsafe.implicits.*
import pureconfig.ConfigReader

import scala.reflect.ClassTag

object ConfigLoader {

  def load[C: ClassTag: ConfigReader](namespace: String): C =
    Configs
      .parseResource("default.conf")
      .map { config =>
        Configs.load[C](config, namespace)
      }
      .unsafeRunSync()

}
