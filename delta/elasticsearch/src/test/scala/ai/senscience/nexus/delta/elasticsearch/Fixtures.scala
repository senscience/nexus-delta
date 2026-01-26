package ai.senscience.nexus.delta.elasticsearch

import ai.senscience.nexus.delta.elasticsearch.context.ElasticSearchContext
import ai.senscience.nexus.delta.elasticsearch.model.contexts
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.sdk.RemoteContextResolutionFixtures
import ai.senscience.nexus.delta.sdk.model.MetadataContextValue
import ai.senscience.nexus.delta.sourcing.stream.PipeChainCompiler
import ai.senscience.nexus.delta.sourcing.stream.pipes.defaultPipes
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*

trait Fixtures extends RemoteContextResolutionFixtures {

  private val elasticSearchContext =
    List("contexts/schemas-metadata.json", "contexts/metadata.json")
      .traverse { file =>
        MetadataContextValue.fromFile(file)
      }
      .map(ElasticSearchContext(_))

  given rcr: RemoteContextResolution =
    loadCoreContexts(contexts.definition).merge(elasticSearchContext.unsafeRunSync().rcr)

  val pipeChainCompiler = PipeChainCompiler(defaultPipes)
}

object Fixtures extends Fixtures
