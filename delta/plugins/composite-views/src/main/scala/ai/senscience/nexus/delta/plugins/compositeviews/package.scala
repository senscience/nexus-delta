package ai.senscience.nexus.delta.plugins

import ai.senscience.nexus.delta.plugins.compositeviews.indexing.CompositeViewDef.ActiveViewDef
import ai.senscience.nexus.delta.sdk.model.IdSegmentRef
import cats.effect.IO
import ch.epfl.bluebrain.nexus.delta.rdf.IriOrBNode.Iri
import ch.epfl.bluebrain.nexus.delta.sourcing.model.ProjectRef

package object compositeviews {

  type FetchView = (IdSegmentRef, ProjectRef) => IO[ActiveViewDef]
  type ExpandId  = (IdSegmentRef, ProjectRef) => IO[Iri]

}
