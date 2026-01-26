package ai.senscience.nexus.delta.elasticsearch.context

import ai.senscience.nexus.delta.elasticsearch.model.contexts
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue.ContextObject
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.sdk.model.MetadataContextValue

final case class ElasticSearchContext(
    searchMetadata: MetadataContextValue,
    indexingMetadata: MetadataContextValue,
    rcr: RemoteContextResolution
)

object ElasticSearchContext {

  def apply(metadataContexts: Iterable[MetadataContextValue]): ElasticSearchContext = {
    val searchMetadata   = metadataContexts.foldLeft(MetadataContextValue.empty)(_.merge(_))
    val indexingMetadata = MetadataContextValue(searchMetadata.value.visit(obj = { case ContextObject(obj) =>
      ContextObject(obj.filterKeys(_.startsWith("_")))
    }))
    val rcr              = RemoteContextResolution.fixed(
      contexts.indexingMetadata -> indexingMetadata.value,
      contexts.searchMetadata   -> searchMetadata.value
    )
    new ElasticSearchContext(searchMetadata, indexingMetadata, rcr)
  }

}
