package ai.senscience.nexus.delta.kernel

import com.typesafe.config.Config
import fs2.io.file.Path

package object config {

  extension (config: Config) {
    def getFilePath(path: String): Path = Path(config.getString(path))

    def getOptionalFilePath(path: String): Option[Path] =
      Option.when(config.hasPath(path))(getFilePath(path))
  }

}
