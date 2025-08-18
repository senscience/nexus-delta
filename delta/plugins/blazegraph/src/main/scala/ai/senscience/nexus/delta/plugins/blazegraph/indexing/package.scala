package ai.senscience.nexus.delta.plugins.blazegraph

import ai.senscience.nexus.delta.plugins.blazegraph.indexing.IndexingViewDef.ActiveViewDef
import ai.senscience.nexus.delta.sdk.model.IdSegment
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import cats.effect.IO

package object indexing {

  type FetchIndexingView = (IdSegment, ProjectRef) => IO[ActiveViewDef]

}
