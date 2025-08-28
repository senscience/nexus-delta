package ai.senscience.nexus.delta.elasticsearch

import ai.senscience.nexus.delta.elasticsearch.client.IndexLabel
import ai.senscience.nexus.delta.elasticsearch.indexing.CurrentActiveViews
import ai.senscience.nexus.delta.elasticsearch.indexing.IndexingViewDef.ActiveViewDef
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.rdf.jsonld.ExpandedJsonLd
import ai.senscience.nexus.delta.sdk.indexing.IndexingAction.IndexingActionContext
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sdk.views.{IndexingRev, ViewRef}
import ai.senscience.nexus.delta.sourcing.PullRequest
import ai.senscience.nexus.delta.sourcing.PullRequest.PullRequestState
import ai.senscience.nexus.delta.sourcing.PullRequest.PullRequestState.PullRequestActive
import ai.senscience.nexus.delta.sourcing.model.Identity.Anonymous
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.model.Tag.UserTag
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.query.SelectFilter
import ai.senscience.nexus.delta.sourcing.state.GraphResource
import ai.senscience.nexus.delta.sourcing.stream.Elem.{FailedElem, SuccessElem}
import ai.senscience.nexus.delta.sourcing.stream.ProjectionErr.CouldNotFindPipeErr
import ai.senscience.nexus.delta.sourcing.stream.{Elem, NoopSink, PipeChain, PipeRef}
import ai.senscience.nexus.testkit.CirceLiteral
import ai.senscience.nexus.testkit.mu.NexusSuite
import ai.senscience.nexus.testkit.mu.ce.PatienceConfig
import io.circe.Json

import java.time.Instant
import scala.concurrent.duration.*

class ElasticSearchIndexingActionSuite extends NexusSuite with CirceLiteral with Fixtures {

  implicit private val patienceConfig: PatienceConfig = PatienceConfig(5.seconds, 10.millis)

  private val instant = Instant.EPOCH

  private val indexingRev = IndexingRev.init
  private val rev         = 2

  private val project = ProjectRef.unsafe("org", "proj")
  private val id1     = nxv + "view1"
  private val view1   = ActiveViewDef(
    ViewRef(project, id1),
    projection = id1.toString,
    None,
    SelectFilter.latest,
    index = IndexLabel.unsafe("view1"),
    mapping = jobj"""{"properties": { }}""",
    settings = jobj"""{"analysis": { }}""",
    None,
    indexingRev,
    rev
  )

  private val id2   = nxv + "view2"
  private val view2 = ActiveViewDef(
    ViewRef(project, id2),
    projection = id2.toString,
    None,
    SelectFilter.tag(UserTag.unsafe("tag")),
    index = IndexLabel.unsafe("view2"),
    mapping = jobj"""{"properties": { }}""",
    settings = jobj"""{"analysis": { }}""",
    None,
    indexingRev,
    rev
  )

  private val id3         = nxv + "view3"
  private val unknownPipe = PipeRef.unsafe("xxx")
  private val view3       = ActiveViewDef(
    ViewRef(project, id3),
    projection = id3.toString,
    Some(PipeChain(PipeRef.unsafe("xxx") -> ExpandedJsonLd.empty)),
    SelectFilter.latest,
    index = IndexLabel.unsafe("view3"),
    mapping = jobj"""{"properties": { }}""",
    settings = jobj"""{"analysis": { }}""",
    None,
    indexingRev,
    rev
  )

  private val currentViews = CurrentActiveViews(view1, view2, view3)

  private val indexingAction = new ElasticSearchIndexingAction(
    currentViews,
    (_: PipeChain) => Left(CouldNotFindPipeErr(unknownPipe)),
    (a: ActiveViewDef) =>
      a.ref.viewId match {
        case `id1` => new NoopSink[Json]
        case `id3` => new NoopSink[Json]
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

  private def index(elem: Elem[GraphResource]) =
    for {
      context <- IndexingActionContext()
      _       <- indexingAction(project, elem, context)
      errors  <- context.get
    } yield errors

  test("Collect only the adequate views") {
    indexingAction
      .projections(project, elem)
      .map(_.metadata.resourceId)
      .assert(Some(id1))
  }

  test("A valid elem should be indexed") {
    index(elem).assertEquals(List.empty)
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

    index(failed).assertEquals(List(failed.throwable))
  }

}
