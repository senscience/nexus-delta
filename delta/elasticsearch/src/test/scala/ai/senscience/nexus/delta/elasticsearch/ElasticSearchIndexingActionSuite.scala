package ai.senscience.nexus.delta.elasticsearch

import ai.senscience.nexus.delta.elasticsearch.client.IndexLabel
import ai.senscience.nexus.delta.elasticsearch.indexing.CurrentActiveViews
import ai.senscience.nexus.delta.elasticsearch.indexing.IndexingViewDef.ActiveViewDef
import ai.senscience.nexus.delta.rdf.Vocabulary
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.rdf.graph.Graph
import ai.senscience.nexus.delta.rdf.jsonld.ExpandedJsonLd
import ai.senscience.nexus.delta.sdk.ResourceShifts
import ai.senscience.nexus.delta.sdk.indexing.sync.SyncIndexingOutcome
import ai.senscience.nexus.delta.sdk.jsonld.JsonLdContent
import ai.senscience.nexus.delta.sdk.model.{ResourceAccess, ResourceF}
import ai.senscience.nexus.delta.sdk.views.{IndexingRev, ViewRef}
import ai.senscience.nexus.delta.sourcing.model.Identity.Anonymous
import ai.senscience.nexus.delta.sourcing.model.Tag.UserTag
import ai.senscience.nexus.delta.sourcing.model.{EntityType, ProjectRef, ResourceRef}
import ai.senscience.nexus.delta.sourcing.query.SelectFilter
import ai.senscience.nexus.delta.sourcing.state.GraphResource
import ai.senscience.nexus.delta.sourcing.stream.*
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.data.NonEmptyList
import cats.effect.IO
import io.circe.Json

import java.time.Instant
import scala.concurrent.duration.*

class ElasticSearchIndexingActionSuite extends NexusSuite with Fixtures {

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

  private val id3   = nxv + "view3"
  private val view3 = ActiveViewDef(
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

  private val entityType = EntityType("test")

  private val id  = nxv + "id1"
  private val res = ResourceF[Unit](
    id = id,
    access = ResourceAccess.resource(project, id),
    rev = 1,
    types = Set(nxv + "Test"),
    deprecated = false,
    createdAt = instant,
    createdBy = Anonymous,
    updatedAt = instant,
    updatedBy = Anonymous,
    schema = ResourceRef(Vocabulary.schemas.resources),
    value = ()
  )

  private val exception = new IllegalStateException("Boom")

  private val shifts = new ResourceShifts {
    override def entityTypes: Option[NonEmptyList[EntityType]] = None

    override def fetch(reference: ResourceRef, project: ProjectRef): IO[Option[JsonLdContent[?]]] = IO.none

    override def decodeGraphResource(entityType: EntityType)(json: Json): IO[GraphResource] = IO.stub

    override def toGraphResource[A](tpe: EntityType)(project: ProjectRef, resource: ResourceF[A]): IO[GraphResource] =
      tpe match {
        case `entityType` =>
          IO.pure(
            GraphResource(
              entityType,
              project,
              id,
              1,
              false,
              ResourceRef(Vocabulary.schemas.resources),
              Set(nxv + "Test"),
              Graph.empty,
              Graph.empty,
              Json.obj()
            )
          )
        case _            => IO.raiseError(exception)
      }
  }

  private val indexingAction = new ElasticSearchIndexingAction(
    shifts,
    currentViews,
    PipeChainCompiler.alwaysFail,
    (a: ActiveViewDef) =>
      a.ref.viewId match {
        case `id1` => new NoopSink[Json]
        case `id3` => new NoopSink[Json]
        case id    => throw new IllegalArgumentException(s"$id should not intent to create a sink")
      },
    5.seconds
  )

  test("A valid elem should be indexed") {
    indexingAction(entityType)(project, res)
      .assertEquals(SyncIndexingOutcome.Success)
  }

  test("A failed elem should be returned") {
    indexingAction(EntityType("xxx"))(project, res)
      .assertEquals(SyncIndexingOutcome.Failed(List(exception)))
  }

}
