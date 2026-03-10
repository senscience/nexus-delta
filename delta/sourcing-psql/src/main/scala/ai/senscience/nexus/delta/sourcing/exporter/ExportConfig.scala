package ai.senscience.nexus.delta.sourcing.exporter

import fs2.io.file.Path
import org.http4s.Uri
import pureconfig.ConfigConvert.catchReadError
import pureconfig.generic.semiauto.deriveReader
import pureconfig.module.http4s.uriReader
import pureconfig.{ConfigConvert, ConfigReader}

final case class ExportConfig(
    batchSize: Int,
    limitPerFile: Int,
    permits: Int,
    target: Path,
    nquads: ExportConfig.NQuadsExportConfig
)

object ExportConfig {

  final case class NQuadsExportConfig(target: Path, graphBaseUri: Uri)

  object NQuadsExportConfig {
    given ConfigReader[NQuadsExportConfig] = {
      given ConfigReader[Path] = ConfigConvert.viaString(catchReadError(s => Path(s)), _.toString)
      deriveReader[NQuadsExportConfig]
    }
  }

  given ConfigReader[ExportConfig] = {
    given ConfigReader[Path] = ConfigConvert.viaString(catchReadError(s => Path(s)), _.toString)
    deriveReader[ExportConfig]
  }

}
