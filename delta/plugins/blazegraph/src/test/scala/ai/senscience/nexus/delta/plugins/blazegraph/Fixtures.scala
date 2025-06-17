package ai.senscience.nexus.delta.plugins.blazegraph

import ai.senscience.nexus.delta.kernel.utils.ClasspathResourceLoader
import ai.senscience.nexus.delta.plugins.blazegraph.model.BlazegraphViewValue
import ai.senscience.nexus.delta.plugins.blazegraph.model.contexts.{blazegraph, blazegraphMetadata}
import ai.senscience.nexus.delta.rdf.Vocabulary
import ai.senscience.nexus.delta.rdf.jsonld.api.{JsonLdApi, TitaniumJsonLdApi}
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import cats.effect.IO

trait Fixtures {

  implicit private val loader: ClasspathResourceLoader = ClasspathResourceLoader()

  implicit val api: JsonLdApi = TitaniumJsonLdApi.strict

  implicit val rcr: RemoteContextResolution = RemoteContextResolution.fixedIO(
    blazegraph                     -> ContextValue.fromFile("contexts/sparql.json"),
    blazegraphMetadata             -> ContextValue.fromFile("contexts/sparql-metadata.json"),
    Vocabulary.contexts.metadata   -> ContextValue.fromFile("contexts/metadata.json"),
    Vocabulary.contexts.error      -> ContextValue.fromFile("contexts/error.json"),
    Vocabulary.contexts.shacl      -> ContextValue.fromFile("contexts/shacl.json"),
    Vocabulary.contexts.statistics -> ContextValue.fromFile("contexts/statistics.json"),
    Vocabulary.contexts.offset     -> ContextValue.fromFile("contexts/offset.json"),
    Vocabulary.contexts.tags       -> ContextValue.fromFile("contexts/tags.json"),
    Vocabulary.contexts.search     -> ContextValue.fromFile("contexts/search.json")
  )

  def alwaysValidate: ValidateBlazegraphView = (_: BlazegraphViewValue) => IO.unit
}

object Fixtures extends Fixtures
