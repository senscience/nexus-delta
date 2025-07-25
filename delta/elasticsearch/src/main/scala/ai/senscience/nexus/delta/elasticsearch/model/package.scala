package ai.senscience.nexus.delta.elasticsearch

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.{contexts as nxvContexts, nxv, schemas}
import ai.senscience.nexus.delta.sdk.model.ResourceF
import ai.senscience.nexus.delta.sdk.permissions.Permissions
import ai.senscience.nexus.delta.sdk.permissions.model.Permission
import ai.senscience.nexus.delta.sourcing.model.ResourceRef
import ai.senscience.nexus.delta.sourcing.model.ResourceRef.Latest

package object model {

  /**
    * Type alias for a view specific resource.
    */
  type ViewResource = ResourceF[ElasticSearchView]

  /**
    * The fixed virtual schema of an ElasticSearchView.
    */
  final val schema: ResourceRef = Latest(schemas + "views.json")

  /**
    * ElasticSearch views contexts.
    */
  object contexts {
    val elasticsearch         = nxvContexts + "elasticsearch.json"
    val elasticsearchMetadata = nxvContexts + "elasticsearch-metadata.json"
    val elasticsearchIndexing = nxvContexts + "elasticsearch-indexing.json"
    val indexingMetadata      = nxvContexts + "indexing-metadata.json"
    val searchMetadata        = nxvContexts + "search-metadata.json"
  }

  object permissions {
    val write: Permission = Permission.unsafe("views/write")
    val read: Permission  = Permissions.resources.read
    val query: Permission = Permission.unsafe("views/query")
  }

  /**
    * The id for the default elasticsearch view
    */
  final val defaultViewId: Iri = nxv + "defaultElasticSearchIndex"
}
