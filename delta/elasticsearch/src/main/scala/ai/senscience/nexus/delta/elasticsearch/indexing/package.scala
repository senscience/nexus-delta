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

  val mainIndexingModule: String = "main-indexing"

  val mainIndexingId: IriOrBNode.Iri = nxv + "main-indexing"

  def mainIndexingProjection(ref: ProjectRef): String = s"main-indexing-$ref"

  def mainIndexingProjectionMetadata(project: ProjectRef): ProjectionMetadata = ProjectionMetadata(
    mainIndexingModule,
    mainIndexingProjection(project),
    Some(project),
    Some(mainIndexingId)
  )

  val configuredIndexingModule: String = "configured-indexing"

  val configuredIndexingId: IriOrBNode.Iri = nxv + "configured-indexing"

  def configuredIndexingProjection(ref: ProjectRef): String = s"configured-indexing-$ref"

  def configuredIndexingProjectionMetadata(project: ProjectRef): ProjectionMetadata = ProjectionMetadata(
    configuredIndexingModule,
    configuredIndexingProjection(project),
    Some(project),
    Some(configuredIndexingId)
  )
}
