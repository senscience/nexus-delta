package ai.senscience.nexus.delta.plugins.blazegraph

import ai.senscience.nexus.delta.plugins.blazegraph.SparqlIndexingActionSuite.{emptyAcc, IdAcc}
import ai.senscience.nexus.delta.plugins.blazegraph.indexing.IndexingViewDef
import ai.senscience.nexus.delta.plugins.blazegraph.indexing.IndexingViewDef.{ActiveViewDef, DeprecatedViewDef}
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.rdf.graph.NTriples
import ai.senscience.nexus.delta.rdf.jsonld.ExpandedJsonLd
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sdk.views.ViewRef
import ai.senscience.nexus.delta.sourcing.PullRequest
import ai.senscience.nexus.delta.sourcing.PullRequest.PullRequestState
import ai.senscience.nexus.delta.sourcing.PullRequest.PullRequestState.PullRequestActive
import ai.senscience.nexus.delta.sourcing.model.Identity.Anonymous
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.model.Tag.UserTag
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.query.SelectFilter
import ai.senscience.nexus.delta.sourcing.stream.Elem.{DroppedElem, FailedElem, SuccessElem}
import ai.senscience.nexus.delta.sourcing.stream.ProjectionErr.CouldNotFindPipeErr
import ai.senscience.nexus.delta.sourcing.stream.{NoopSink, PipeChain, PipeRef, SuccessElemStream}
import ai.senscience.nexus.testkit.mu.NexusSuite
import ai.senscience.nexus.testkit.mu.ce.PatienceConfig
import fs2.Stream

import java.time.Instant
import scala.concurrent.duration.*

class SparqlIndexingActionSuite extends NexusSuite with Fixtures {

  implicit private val patienceConfig: PatienceConfig = PatienceConfig(5.seconds, 10.millis)

  private val instant     = Instant.EPOCH
  private val indexingRev = 1
  private val currentRev  = 1

  private val project = ProjectRef.unsafe("org", "proj")
  private val id1     = nxv + "view1"
  private val view1   = ActiveViewDef(
    ViewRef(project, id1),
    projection = id1.toString,
    SelectFilter.latest,
    None,
    namespace = "view1",
    indexingRev,
    currentRev
  )

  private val id2   = nxv + "view2"
  private val view2 = ActiveViewDef(
    ViewRef(project, id2),
    projection = id2.toString,
    SelectFilter.tag(UserTag.unsafe("tag")),
    None,
    namespace = "view2",
    indexingRev,
    currentRev
  )

  private val id3         = nxv + "view3"
  private val unknownPipe = PipeRef.unsafe("xxx")
  private val view3       = ActiveViewDef(
    ViewRef(project, id3),
    projection = id3.toString,
    SelectFilter.latest,
    Some(PipeChain(PipeRef.unsafe("xxx") -> ExpandedJsonLd.empty)),
    namespace = "view3",
    indexingRev,
    currentRev
  )

  private val id4   = nxv + "view4"
  private val view4 = DeprecatedViewDef(
    ViewRef(project, id4)
  )

  private def viewStream: SuccessElemStream[IndexingViewDef] =
    Stream(
      SuccessElem(
        tpe = BlazegraphViews.entityType,
        id = view1.ref.viewId,
        project = project,
        instant = Instant.EPOCH,
        offset = Offset.at(1L),
        value = view1,
        rev = 1
      ),
      SuccessElem(
        tpe = BlazegraphViews.entityType,
        id = view2.ref.viewId,
        project = project,
        instant = Instant.EPOCH,
        offset = Offset.at(2L),
        value = view2,
        rev = 1
      ),
      SuccessElem(
        tpe = BlazegraphViews.entityType,
        id = view3.ref.viewId,
        project = project,
        instant = Instant.EPOCH,
        offset = Offset.at(3L),
        value = view3,
        rev = 1
      ),
      SuccessElem(
        tpe = BlazegraphViews.entityType,
        id = view4.ref.viewId,
        project = project,
        instant = Instant.EPOCH,
        offset = Offset.at(4L),
        value = view4,
        rev = 1
      )
    )

  private val indexingAction = new SparqlIndexingAction(
    _ => viewStream,
    (_: PipeChain) => Left(CouldNotFindPipeErr(unknownPipe)),
    (a: ActiveViewDef) =>
      a.ref.viewId match {
        case `id1` => new NoopSink[NTriples]
        case `id3` => new NoopSink[NTriples]
        case id    => throw new IllegalArgumentException(s"$id should not intent to create a sink")
      },
    patienceConfig.timeout
  )

  private val base = iri"http://localhost"
  private val pr   = PullRequestActive(
    id = nxv + "id1",
    project = project,
    rev = 1,
    createdAt = instant,
    createdBy = Anonymous,
    updatedAt = instant,
    updatedBy = Anonymous
  )

  private val elem = SuccessElem(
    tpe = PullRequest.entityType,
    id = pr.id,
    project = project,
    instant = pr.updatedAt,
    offset = Offset.at(1L),
    value = PullRequestState.toGraphResource(pr, base),
    rev = 1
  )

  test("Collect only the adequate views") {
    val expected = IdAcc(Set(id1), Set(id2, id4), Set(id3))

    indexingAction
      .projections(project, elem)
      .fold(emptyAcc) {
        case (acc, s: SuccessElem[?]) => acc.success(s.id)
        case (acc, d: DroppedElem)    => acc.drop(d.id)
        case (acc, f: FailedElem)     => acc.failed(f.id)
      }
      .compile
      .lastOrError
      .assertEquals(expected)
  }

  test("A valid elem should be indexed") {
    indexingAction.apply(project, elem).assertEquals(List.empty)
  }

  test("A failed elem should be returned") {
    val failed = FailedElem(
      tpe = PullRequest.entityType,
      id = pr.id,
      project = project,
      instant = pr.updatedAt,
      offset = Offset.at(1L),
      new IllegalStateException("Boom"),
      rev = 1
    )

    indexingAction.apply(project, failed).assertEquals(List(failed))
  }

}

object SparqlIndexingActionSuite {

  final private case class IdAcc(successes: Set[Iri], dropped: Set[Iri], failures: Set[Iri]) {
    def success(id: Iri): IdAcc = this.copy(successes = successes + id)
    def drop(id: Iri): IdAcc    = this.copy(dropped = dropped + id)
    def failed(id: Iri): IdAcc  = this.copy(failures = failures + id)
  }

  private val emptyAcc = IdAcc(Set.empty, Set.empty, Set.empty)

}
