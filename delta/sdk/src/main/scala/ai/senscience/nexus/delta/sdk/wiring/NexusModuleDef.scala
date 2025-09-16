package ai.senscience.nexus.delta.sdk.wiring

import ai.senscience.nexus.delta.kernel.config.Configs
import com.typesafe.config.Config
import distage.{ModuleDef, Tag}
import izumi.distage.model.definition.dsl.ModuleDefDSL.MakeDSLUnnamedAfterFrom
import pureconfig.ConfigReader

import scala.reflect.ClassTag

trait NexusModuleDef extends ModuleDef {

  final def makeConfig[C: ClassTag: ConfigReader: Tag](path: String): MakeDSLUnnamedAfterFrom[C] =
    make[C].from(Configs.load[C](_, path))

}
