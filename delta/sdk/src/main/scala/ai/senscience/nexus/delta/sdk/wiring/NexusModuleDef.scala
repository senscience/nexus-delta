package ai.senscience.nexus.delta.sdk.wiring

import ai.senscience.nexus.delta.kernel.config.Configs
import cats.effect.IO
import com.typesafe.config.Config
import distage.{ModuleDef, Tag}
import izumi.distage.model.definition.dsl.ModuleDefDSL
import izumi.distage.model.definition.dsl.ModuleDefDSL.MakeDSLUnnamedAfterFrom
import org.typelevel.otel4s.oteljava.OtelJava
import org.typelevel.otel4s.trace.Tracer
import pureconfig.ConfigReader

import scala.reflect.ClassTag

trait NexusModuleDef extends ModuleDef {

  final def makeConfig[C: ClassTag: ConfigReader: Tag](path: String): MakeDSLUnnamedAfterFrom[C] =
    make[C].from(Configs.load[C](_, path))

  final def makeTracer(name: String): ModuleDefDSL.MakeDSLNamedAfterFrom[Tracer[IO]] =
    make[Tracer[IO]].named(name).fromEffect { (otel: OtelJava[IO]) =>
      otel.tracerProvider.get(name)
    }

}
