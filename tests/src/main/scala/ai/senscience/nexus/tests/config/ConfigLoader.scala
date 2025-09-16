package ai.senscience.nexus.tests.config

import ai.senscience.nexus.delta.kernel.config.Configs
import com.typesafe.config.Config
import pureconfig.ConfigReader

import scala.reflect.ClassTag

trait ConfigLoader {

  def load[A: ClassTag](config: Config, namespace: String)(implicit reader: ConfigReader[A]): A =
    Configs.load[A](config, namespace)

}

object ConfigLoader extends ConfigLoader
