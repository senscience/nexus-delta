package ai.senscience.nexus.tests.config

import ai.senscience.nexus.delta.kernel.config.Configs
import com.typesafe.config.Config
import org.apache.pekko.http.scaladsl.model.Uri
import pureconfig.ConvertHelpers.catchReadError
import pureconfig.generic.ExportMacros
import pureconfig.{ConfigConvert, ConfigReader, Exported}

import scala.reflect.ClassTag

trait ConfigLoader {

  implicit def exportReader[A]: Exported[ConfigReader[A]] = macro ExportMacros.exportDerivedReader[A]

  implicit val uriConverter: ConfigConvert[Uri] =
    ConfigConvert.viaString[Uri](catchReadError(s => Uri(s)), _.toString)

  def load[A: ClassTag](config: Config, namespace: String)(implicit reader: ConfigReader[A]): A =
    Configs.load[A](config, namespace)

}

object ConfigLoader extends ConfigLoader
