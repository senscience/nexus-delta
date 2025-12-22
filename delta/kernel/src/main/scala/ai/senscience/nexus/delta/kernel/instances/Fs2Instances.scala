package ai.senscience.nexus.delta.kernel.instances

import fs2.io.file.Path
import pureconfig.{ConfigReader, ConvertHelpers}

trait Fs2Instances extends ConvertHelpers {

  given ConfigReader[Path] = ConfigReader.fromString[Path](catchReadError(Path(_)))

}
