package ai.senscience.nexus.delta.plugins.elasticsearch.query

import ai.senscience.nexus.delta.kernel.search.Pagination
import ai.senscience.nexus.delta.plugins.elasticsearch.model.ResourcesSearchParams
import ai.senscience.nexus.delta.sdk.model.search.SortList

/**
  * Search request on the main index
  */
final case class MainIndexRequest(params: ResourcesSearchParams, pagination: Pagination, sort: SortList)
    extends Product
    with Serializable
