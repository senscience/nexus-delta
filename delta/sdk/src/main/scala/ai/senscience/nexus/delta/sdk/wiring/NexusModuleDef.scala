package ai.senscience.nexus.delta.sdk.wiring

import ai.senscience.nexus.delta.kernel.config.Configs
import ai.senscience.nexus.delta.kernel.utils.ClasspathResourceLoader
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.sourcing.model.EntityType
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
    make[C].fromEffect { (config: Config) =>
      Configs.loadEffect[C](config, path)
    }

  final def makeTracer(name: String): ModuleDefDSL.MakeDSLNamedAfterFrom[Tracer[IO]] =
    make[Tracer[IO]].named(name).fromEffect { (otel: OtelJava[IO]) =>
      otel.tracerProvider.get(name)
    }

  final def addRemoteContextResolution(
      contexts: Set[(Iri, String)]
  )(using loader: ClasspathResourceLoader): ModuleDefDSL.SetElementDSL[RemoteContextResolution] =
    many[RemoteContextResolution].addEffect(
      RemoteContextResolution.loadResources(contexts)
    )

  final def addIndexingType(entityType: EntityType): ModuleDefDSL.SetElementDSL[EntityType] =
    many[EntityType].named("indexing-types").add(entityType)

}
