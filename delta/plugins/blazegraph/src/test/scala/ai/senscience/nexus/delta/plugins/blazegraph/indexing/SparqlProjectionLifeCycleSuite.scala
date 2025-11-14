package ai.senscience.nexus.delta.plugins.blazegraph.indexing

import ai.senscience.nexus.delta.kernel.RetryStrategyConfig
import ai.senscience.nexus.delta.plugins.blazegraph.SparqlClientSetup
import ai.senscience.nexus.delta.plugins.blazegraph.client.SparqlClient
import ai.senscience.nexus.delta.plugins.blazegraph.indexing.IndexingViewDef.ActiveViewDef
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.stream.GraphResourceStream
import ai.senscience.nexus.delta.sdk.views.ViewRef
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.query.SelectFilter
import ai.senscience.nexus.delta.sourcing.stream.config.BatchConfig
import ai.senscience.nexus.delta.sourcing.stream.{PipeChainCompiler, PullRequestStream}
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.effect.IO
import fs2.concurrent.Signal
import munit.AnyFixture

class SparqlProjectionLifeCycleSuite extends NexusSuite with SparqlClientSetup.Fixture {

  override def munitFixtures: Seq[AnyFixture[?]] = List(blazegraphClient)

  implicit private val baseUri: BaseUri = BaseUri.unsafe("http://localhost", "v1")

  private lazy val client: SparqlClient = blazegraphClient()

  private val project = ProjectRef.unsafe("org", "proj")

  private val id   = nxv + "view1"
  private val view = ActiveViewDef(
    ViewRef(project, id),
    projection = id.toString,
    SelectFilter.latest,
    None,
    namespace = "view1",
    1,
    1
  )

  private val healthCheck: SparqlHealthCheck = new SparqlHealthCheck {
    override def healthy: Signal[IO, Boolean] = Signal.constant(true)

    override def failing: Signal[IO, Boolean] = Signal.constant(false)
  }

  private lazy val projectionLifeCycle =
    SparqlProjectionLifeCycle(
      GraphResourceStream.unsafeFromStream(PullRequestStream.generate(project)),
      PipeChainCompiler.alwaysFail,
      healthCheck,
      client,
      RetryStrategyConfig.AlwaysGiveUp,
      BatchConfig.individual
    )

  test("Compile the indexing view") {
    projectionLifeCycle.compile(view).map { projection =>
      assertEquals(projection.metadata.name, view.projection)
    }
  }

  test("Create the namespace") {
    projectionLifeCycle.init(view) >>
      client.existsNamespace(view.namespace).assertEquals(true)
  }

  test("Delete the namespace") {
    projectionLifeCycle.destroy(view) >>
      client.existsNamespace(view.namespace).assertEquals(false)
  }
}
