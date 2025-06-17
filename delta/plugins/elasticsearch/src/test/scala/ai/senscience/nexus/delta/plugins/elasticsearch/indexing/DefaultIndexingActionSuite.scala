package ai.senscience.nexus.delta.plugins.elasticsearch.indexing

import ai.senscience.nexus.delta.plugins.elasticsearch.Fixtures
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.rdf.syntax.iriStringContextSyntax
import ai.senscience.nexus.delta.sourcing.PullRequest
import ai.senscience.nexus.delta.sourcing.PullRequest.PullRequestState
import ai.senscience.nexus.delta.sourcing.PullRequest.PullRequestState.PullRequestActive
import ai.senscience.nexus.delta.sourcing.model.Identity.Anonymous
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.stream.Elem.{FailedElem, SuccessElem}
import ai.senscience.nexus.delta.sourcing.stream.NoopSink
import ai.senscience.nexus.testkit.mu.NexusSuite
import ai.senscience.nexus.testkit.mu.ce.PatienceConfig
import io.circe.Json

import java.time.Instant
import scala.concurrent.duration.*

class DefaultIndexingActionSuite extends NexusSuite with Fixtures {

  implicit private val patienceConfig: PatienceConfig = PatienceConfig(5.seconds, 10.millis)

  private val instant = Instant.EPOCH
  private val project = ProjectRef.unsafe("org", "proj")
  private val base    = iri"http://localhost"

  private val pr = PullRequestActive(
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

  private val defaultIndexingAction = new MainIndexingAction(new NoopSink[Json], patienceConfig.timeout)

  test("A valid elem should be indexed") {
    defaultIndexingAction(project, elem).assertEquals(List.empty)
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

    defaultIndexingAction(project, failed).assertEquals(List(failed))
  }
}
