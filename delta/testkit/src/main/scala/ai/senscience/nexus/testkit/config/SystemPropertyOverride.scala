package ai.senscience.nexus.testkit.config

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.typesafe.config.impl.ConfigImpl

object SystemPropertyOverride {

  private def reload: IO[Unit]                               = IO.delay(ConfigImpl.reloadSystemPropertiesConfig())
  def apply(io: IO[Map[String, String]]): Resource[IO, Unit] = {
    def acquire                                        = io.flatTap { values =>
      IO.delay(values.foreach { case (k, v) => System.setProperty(k, v) }) >> reload
    }
    def release(values: Map[String, String]): IO[Unit] = IO.delay(
      values.foreach { case (k, _) => System.clearProperty(k) }
    )
    Resource.make(acquire)(release).void
  }

  def apply(values: Map[String, String]): Resource[IO, Unit] = apply(IO.pure(values))

}
