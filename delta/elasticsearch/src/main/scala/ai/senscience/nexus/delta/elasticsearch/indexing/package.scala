package ai.senscience.nexus.delta.elasticsearch

import ai.senscience.nexus.delta.elasticsearch.indexing.IndexingViewDef.ActiveViewDef
import ai.senscience.nexus.delta.elasticsearch.model.contexts
import ai.senscience.nexus.delta.rdf.IriOrBNode
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue
import ai.senscience.nexus.delta.sdk.model.IdSegment
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.stream.ProjectionMetadata
import cats.effect.IO

package object indexing {

  type FetchIndexingView = (IdSegment, ProjectRef) => IO[ActiveViewDef]

  val defaultIndexingContext: ContextValue = ContextValue(contexts.elasticsearchIndexing, contexts.indexingMetadata)

  val mainIndexingId: IriOrBNode.Iri = nxv + "main-indexing"

  def mainIndexingProjection(ref: ProjectRef): String = s"main-indexing-$ref"

  def mainIndexingProjectionMetadata(project: ProjectRef): ProjectionMetadata = ProjectionMetadata(
    "main-indexing",
    mainIndexingProjection(project),
    Some(project),
    Some(mainIndexingId)
  )
}
