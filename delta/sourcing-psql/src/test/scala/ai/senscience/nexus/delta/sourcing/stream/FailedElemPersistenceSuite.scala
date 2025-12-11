package ai.senscience.nexus.delta.sourcing.stream

import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sourcing.model.{EntityType, ProjectRef}
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.stream.Elem.*
import ai.senscience.nexus.delta.sourcing.stream.config.BatchConfig
import ai.senscience.nexus.testkit.mu.NexusSuite
import ai.senscience.nexus.testkit.mu.ce.PatienceConfig
import cats.effect.IO
import fs2.Stream
import munit.Location

import java.time.Instant
import scala.concurrent.duration.DurationInt

class FailedElemPersistenceSuite extends NexusSuite {

  private given BatchConfig    = BatchConfig(2, 10.millis)
  private given PatienceConfig = PatienceConfig(500.millis, 10.millis)

  private val projection1 = ProjectionMetadata("test", "name1", None, None)
  private val project     = ProjectRef.unsafe("org", "proj")
  private val id          = nxv + "id"
  private val rev         = 1

  private def failureStream =
    (_: Offset) =>
      Stream
        .range(1, 11)
        .map { value =>
          FailedElem(
            EntityType("entity"),
            id,
            project,
            Instant.EPOCH,
            Offset.at(value.toLong),
            new RuntimeException("boom"),
            rev
          )
        }

  private def successStream =
    (_: Offset) =>
      Stream
        .range(1, 11)
        .map { value =>
          SuccessElem(EntityType("entity"), id, project, Instant.EPOCH, Offset.at(value.toLong), (), rev)
        }

  private val cpPersistentNodeFailures  =
    CompiledProjection.fromStream(projection1, ExecutionStrategy.PersistentSingleNode, failureStream)
  private val cpEveryNodeFailures       =
    CompiledProjection.fromStream(projection1, ExecutionStrategy.EveryNode, failureStream)
  private val cpPersistentNodeSuccesses =
    CompiledProjection.fromStream(projection1, ExecutionStrategy.PersistentSingleNode, successStream)

  private def runProjectionAndSaveErrors(compiled: CompiledProjection, expectedErrors: Int)(using location: Location) =
    for {
      failedElems <- IO.ref(List.empty[FailedElem])
      projection  <- {
        def saveErrors(errors: List[FailedElem]) = failedElems.update(_ ++ errors)
        Projection.apply(compiled, IO.none, _ => IO.unit, saveErrors, _ => IO.unit)
      }
      _           <- projection.executionStatus.assertEquals(ExecutionStatus.Completed).eventually
      _           <- failedElems.get.map(_.size).assertEquals(expectedErrors)
    } yield ()

  test("FailedElems are saved (persistent single node)") {
    runProjectionAndSaveErrors(cpPersistentNodeFailures, 10)
  }

  test("FailedElems are saved (every node)") {
    runProjectionAndSaveErrors(cpEveryNodeFailures, 10)
  }

  test("Success stream saves no FailedElems") {
    runProjectionAndSaveErrors(cpPersistentNodeSuccesses, 0)
  }

}
