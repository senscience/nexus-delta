package ai.senscience.nexus.delta.sourcing.exporter

import fs2.io.file.Path
import pureconfig.ConfigConvert.catchReadError
import pureconfig.generic.semiauto.deriveReader
import pureconfig.{ConfigConvert, ConfigReader}

final case class ExportConfig(batchSize: Int, limitPerFile: Int, permits: Int, target: Path)

object ExportConfig {

  given ConfigReader[ExportConfig] = {
    given ConfigReader[Path] = ConfigConvert.viaString(catchReadError(s => Path(s)), _.toString)
    deriveReader[ExportConfig]
  }

}
