package ai.senscience.nexus.delta.elasticsearch

import ai.senscience.nexus.delta.elasticsearch.model.contexts
import ai.senscience.nexus.delta.rdf.jsonld.api.{JsonLdApi, TitaniumJsonLdApi}
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue.ContextObject
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import ai.senscience.nexus.delta.sdk.RemoteContextResolutionFixtures
import ai.senscience.nexus.delta.sourcing.stream.PipeChainCompiler
import ai.senscience.nexus.delta.sourcing.stream.pipes.defaultPipes
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*

trait Fixtures extends RemoteContextResolutionFixtures {

  private val listingsMetadataCtx =
    List(
      "contexts/acls-metadata.json",
      "contexts/realms-metadata.json",
      "contexts/organizations-metadata.json",
      "contexts/projects-metadata.json",
      "contexts/resolvers-metadata.json",
      "contexts/schemas-metadata.json",
      "contexts/elasticsearch-metadata.json",
      "contexts/metadata.json"
    ).foldLeftM(ContextValue.empty) { case (acc, file) =>
      ContextValue.fromFile(file).map(acc.merge)
    }

  private val indexingMetadataCtx = listingsMetadataCtx.map(_.visit(obj = { case ContextObject(obj) =>
    ContextObject(obj.filterKeys(_.startsWith("_")))
  }))

  implicit val api: JsonLdApi = TitaniumJsonLdApi.strict

  given rcr: RemoteContextResolution =
    loadCoreContexts(contexts.definition).merge(
      RemoteContextResolution.fixed(
        contexts.searchMetadata   -> listingsMetadataCtx.unsafeRunSync(),
        contexts.indexingMetadata -> indexingMetadataCtx.unsafeRunSync()
      )
    )

  val pipeChainCompiler = PipeChainCompiler(defaultPipes)
}
