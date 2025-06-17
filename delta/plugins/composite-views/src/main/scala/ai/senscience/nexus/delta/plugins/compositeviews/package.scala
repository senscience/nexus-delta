package ai.senscience.nexus.delta.plugins

import ai.senscience.nexus.delta.plugins.compositeviews.indexing.CompositeViewDef.ActiveViewDef
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sdk.model.IdSegmentRef
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import cats.effect.IO

package object compositeviews {

  type FetchView = (IdSegmentRef, ProjectRef) => IO[ActiveViewDef]
  type ExpandId  = (IdSegmentRef, ProjectRef) => IO[Iri]

}
