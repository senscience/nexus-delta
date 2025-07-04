package ai.senscience.nexus.delta.plugins.compositeviews

import ai.senscience.nexus.delta.plugins.compositeviews.indexing.CompositeViewDef.ActiveViewDef
import ai.senscience.nexus.delta.plugins.compositeviews.model.CompositeViewRejection.{InvalidCompositeViewId, ViewNotFound}
import ai.senscience.nexus.delta.sdk.model.IdSegment.{IriSegment, StringSegment}
import ai.senscience.nexus.delta.sdk.model.IdSegmentRef
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import cats.effect.IO

package object test {

  def expectIndexingView(expected: ActiveViewDef, stringSegment: String): FetchView =
    expectIndexingView(expected, Some(stringSegment))

  def expectIndexingView(expected: ActiveViewDef): FetchView =
    expectIndexingView(expected, None)

  def expectIndexingView(expected: ActiveViewDef, stringSegment: Option[String]): FetchView = {
    val expectedId      = expected.id
    val expectedProject = expected.project
    (id: IdSegmentRef, project: ProjectRef) =>
      (id.value, project) match {
        case (IriSegment(`expectedId`), `expectedProject`)                              => IO.pure(expected)
        case (IriSegment(iri), p)                                                       => IO.raiseError(ViewNotFound(iri, p))
        case (StringSegment(value), `expectedProject`) if stringSegment.contains(value) => IO.pure(expected)
        case (StringSegment(value), _)                                                  => IO.raiseError(InvalidCompositeViewId(value))
      }
  }

  def expandOnlyIris: ExpandId = (id: IdSegmentRef, p: ProjectRef) =>
    (id.value, p) match {
      case (IriSegment(iri), _) => IO.pure(iri)
      case (segment, _)         => IO.raiseError(InvalidCompositeViewId(segment.asString))
    }

}
