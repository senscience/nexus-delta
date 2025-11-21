package ai.senscience.nexus.delta.plugins.blazegraph.indexing

import ai.senscience.nexus.delta.plugins.blazegraph.BlazegraphViews
import ai.senscience.nexus.delta.plugins.blazegraph.indexing.IndexingViewDef.{ActiveViewDef, DeprecatedViewDef}
import ai.senscience.nexus.delta.plugins.blazegraph.model.BlazegraphViewValue.{AggregateBlazegraphViewValue, IndexingBlazegraphViewValue}
import ai.senscience.nexus.delta.plugins.blazegraph.model.{BlazegraphViewState, BlazegraphViewValue}
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.rdf.graph.NTriples
import ai.senscience.nexus.delta.rdf.jsonld.ExpandedJsonLd
import ai.senscience.nexus.delta.sdk.views.ViewRef
import ai.senscience.nexus.delta.sourcing.model.Identity.{Anonymous, Subject}
import ai.senscience.nexus.delta.sourcing.model.Tag.UserTag
import ai.senscience.nexus.delta.sourcing.model.{IriFilter, ProjectRef}
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.stream.*
import ai.senscience.nexus.delta.sourcing.stream.ProjectionErr.CouldNotFindTypedPipeErr
import ai.senscience.nexus.delta.sourcing.stream.config.BatchConfig
import ai.senscience.nexus.delta.sourcing.stream.pipes.FilterByType.FilterByTypeConfig
import ai.senscience.nexus.delta.sourcing.stream.pipes.{FilterByType, FilterDeprecated}
import ai.senscience.nexus.testkit.mu.NexusSuite
import ai.senscience.nexus.testkit.mu.ce.PatienceConfig
import cats.data.NonEmptySet
import cats.effect.IO
import fs2.Stream
import io.circe.Json

import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.*

class IndexingViewDefSuite extends NexusSuite {

  private given ProjectionBackpressure = ProjectionBackpressure.Noop
  private given PatienceConfig         = PatienceConfig(500.millis, 10.millis)
  private given BatchConfig            = BatchConfig(2, 10.millis)

  private val prefix = "prefix"

  private val uuid             = UUID.fromString("f8468909-a797-4b10-8b5f-000cba337bfa")
  private val instant: Instant = Instant.EPOCH
  private val projectRef       = ProjectRef.unsafe("myorg", "myproj")
  private val id               = nxv + "indexing-view"
  private val viewRef          = ViewRef(projectRef, id)
  private val subject: Subject = Anonymous
  private val indexingRev      = 1
  private val currentRev       = 1

  private val namespace = s"${prefix}_${uuid}_1"

  private val filterByTypeConfig = FilterByTypeConfig(IriFilter.restrictedTo(nxv + "PullRequest"))
  private val indexing           = IndexingBlazegraphViewValue(
    resourceTag = Some(UserTag.unsafe("some.tag")),
    resourceTypes = IriFilter.restrictedTo(nxv + "PullRequest")
  )

  private val aggregate =
    AggregateBlazegraphViewValue(Some("viewName"), Some("viewDescription"), NonEmptySet.of(viewRef))
  private val sink      = CacheSink.states[NTriples]

  private def state(v: BlazegraphViewValue) = BlazegraphViewState(
    id,
    projectRef,
    uuid,
    v,
    Json.obj("blazegraph" -> Json.fromString("value")),
    rev = 1,
    indexingRev = indexingRev,
    deprecated = false,
    createdAt = instant,
    createdBy = subject,
    updatedAt = instant,
    updatedBy = subject
  )

  test("Build an active view def") {
    assertEquals(
      IndexingViewDef(state(indexing), prefix),
      Some(
        ActiveViewDef(
          viewRef,
          s"blazegraph-$projectRef-$id-$indexingRev",
          indexing.selectFilter,
          indexing.pipeChain,
          namespace,
          indexingRev,
          currentRev
        )
      )
    )
  }

  test("Build an deprecated view def") {
    assertEquals(
      IndexingViewDef(state(indexing).copy(deprecated = true), prefix),
      Some(DeprecatedViewDef(viewRef))
    )
  }

  test("Ignore aggregate views") {
    assertEquals(
      IndexingViewDef(state(aggregate), prefix),
      None
    )
  }

  test("Fail if the pipe chain does not compile") {
    val v = ActiveViewDef(
      viewRef,
      s"blazegraph-$projectRef-$id-1",
      indexing.selectFilter,
      Some(PipeChain(PipeRef.unsafe("xxx") -> ExpandedJsonLd.empty)),
      namespace,
      indexingRev,
      currentRev
    )

    val expectedError = CouldNotFindTypedPipeErr(PipeRef.unsafe("xxx"), "xxx")

    IndexingViewDef
      .compile(v, PipeChainCompiler.alwaysFail, Stream.empty, sink)
      .interceptEquals(expectedError)

    assert(
      sink.successes.isEmpty && sink.dropped.isEmpty && sink.failed.isEmpty,
      "No elem should have been processed"
    )

  }

  test("Success and be able to process the different elements") {
    val v = ActiveViewDef(
      viewRef,
      s"blazegraph-$projectRef-$id-1",
      indexing.selectFilter,
      Some(PipeChain(FilterDeprecated())),
      namespace,
      indexingRev,
      currentRev
    )

    val expectedProgress: ProjectionProgress = ProjectionProgress(
      Offset.at(4L),
      Instant.EPOCH,
      processed = 4,
      discarded = 2,
      failed = 1
    )

    for {
      compiled   <- IndexingViewDef.compile(
                      v,
                      _ => Operation.merge(FilterDeprecated.withConfig(()), FilterByType.withConfig(filterByTypeConfig)),
                      PullRequestStream.generate(projectRef),
                      sink
                    )
      _           = assertEquals(
                      compiled.metadata,
                      ProjectionMetadata(BlazegraphViews.entityType.value, v.projection, Some(projectRef), Some(id))
                    )
      projection <- Projection(compiled, IO.none, _ => IO.unit, _ => IO.unit)
      _          <- projection.executionStatus.assertEquals(ExecutionStatus.Completed).eventually
      _          <- projection.currentProgress.assertEquals(expectedProgress)
    } yield ()
  }

}
