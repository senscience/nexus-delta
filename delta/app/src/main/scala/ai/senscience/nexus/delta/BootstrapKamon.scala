package ai.senscience.nexus.delta

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.kernel.kamon.KamonMonitoring
import ai.senscience.nexus.delta.kernel.utils.IOFuture
import cats.effect.kernel.Resource
import cats.effect.IO
import com.typesafe.config.Config
import kamon.Kamon

import scala.concurrent.duration.DurationInt

object BootstrapKamon {

  private val logger = Logger[BootstrapKamon.type]

  def apply(config: Config): Resource[IO, Unit] = {
    if KamonMonitoring.enabled then {
      val acquire = logger.info("Initializing Kamon") >> IO.blocking(Kamon.init(config))
      val release = IOFuture
        .defaultCancelable { IO(Kamon.stopModules()) }
        .timeout(15.seconds)
        .onError { case e =>
          logger.error(e)("Something went wrong while terminating Kamon")
        }
        .void
      Resource.make(acquire)(_ => release)
    } else Resource.unit
  }

}
