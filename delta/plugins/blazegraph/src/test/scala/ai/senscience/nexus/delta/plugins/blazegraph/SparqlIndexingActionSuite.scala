package ai.senscience.nexus.delta.plugins.blazegraph

import ai.senscience.nexus.delta.plugins.blazegraph.indexing.CurrentActiveViews
import ai.senscience.nexus.delta.plugins.blazegraph.indexing.IndexingViewDef.ActiveViewDef
import ai.senscience.nexus.delta.rdf.Vocabulary
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.rdf.graph.{Graph, NTriples}
import ai.senscience.nexus.delta.rdf.jsonld.ExpandedJsonLd
import ai.senscience.nexus.delta.sdk.GraphResourceEncoder
import ai.senscience.nexus.delta.sdk.indexing.sync.SyncIndexingOutcome
import ai.senscience.nexus.delta.sdk.model.{ResourceAccess, ResourceF}
import ai.senscience.nexus.delta.sdk.views.ViewRef
import ai.senscience.nexus.delta.sourcing.model.Identity.Anonymous
import ai.senscience.nexus.delta.sourcing.model.Tag.UserTag
import ai.senscience.nexus.delta.sourcing.model.{EntityType, ProjectRef, ResourceRef}
import ai.senscience.nexus.delta.sourcing.query.SelectFilter
import ai.senscience.nexus.delta.sourcing.state.GraphResource
import ai.senscience.nexus.delta.sourcing.stream.*
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.effect.IO
import io.circe.Json

import java.time.Instant
import scala.concurrent.duration.*

class SparqlIndexingActionSuite extends NexusSuite with Fixtures {

  private val instant     = Instant.EPOCH
  private val indexingRev = 1
  private val currentRev  = 1

  private val project = ProjectRef.unsafe("org", "proj")

  private def createView(suffix: String, pipeChain: Option[PipeChain], withTag: Boolean) = {
    val id           = nxv + suffix
    val selectFilter = if withTag then SelectFilter.tag(UserTag.unsafe("tag")) else SelectFilter.latest
    ActiveViewDef(
      ViewRef(project, id),
      projection = id.toString,
      selectFilter,
      pipeChain,
      namespace = suffix,
      indexingRev,
      currentRev
    )
  }

  private val view1 = createView("view1", None, withTag = false)
  private val view2 = createView("view2", None, withTag = true)
  private val view3 = createView(
    "view3",
    Some(PipeChain(PipeRef.unsafe("xxx") -> ExpandedJsonLd.empty)),
    withTag = false
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

  private val graphResourceEncoder: GraphResourceEncoder = new GraphResourceEncoder {
    override def encodeResource[A](tpe: EntityType)(project: ProjectRef, resource: ResourceF[A]): IO[GraphResource] = {
      if tpe == `entityType` then
        IO.pure(
          GraphResource(
            entityType,
            project,
            resource.id,
            resource.rev,
            resource.deprecated,
            resource.schema,
            resource.types,
            Graph.empty,
            Graph.empty,
            Json.obj()
          )
        )
      else IO.raiseError(exception)
    }
  }

  private val indexingAction = new SparqlIndexingAction(
    graphResourceEncoder,
    currentViews,
    PipeChainCompiler.alwaysFail,
    (a: ActiveViewDef) =>
      if a.ref == view1.ref || a.ref == view3.ref then new NoopSink[NTriples]
      else throw new IllegalArgumentException(s"${a.ref} should not intent to create a sink"),
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
