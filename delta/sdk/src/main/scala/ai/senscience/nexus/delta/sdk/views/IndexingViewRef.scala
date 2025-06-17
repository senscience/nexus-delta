package ai.senscience.nexus.delta.sdk.views

import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ch.epfl.bluebrain.nexus.delta.rdf.IriOrBNode.Iri

/**
  * View reference for indexing purposes
  *
  * @param ref
  *   the id and the project of the view
  * @param indexingRev
  *   the indexing revision
  */
final case class IndexingViewRef(ref: ViewRef, indexingRev: IndexingRev) {
  def project: ProjectRef = ref.project

  def id: Iri = ref.viewId
}

object IndexingViewRef {

  def apply(project: ProjectRef, id: Iri, indexingRev: IndexingRev): IndexingViewRef =
    IndexingViewRef(ViewRef(project, id), indexingRev)
}
