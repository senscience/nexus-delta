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

import java.time.Instant
import scala.collection.mutable.Set as MutableSet
import scala.concurrent.duration.DurationInt

class FailedElemPersistenceSuite extends NexusSuite {

  private given ProjectionBackpressure = ProjectionBackpressure.Noop
  private given BatchConfig            = BatchConfig(2, 10.millis)
  private given PatienceConfig         = PatienceConfig(500.millis, 10.millis)

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

  private val saveFailedElems: MutableSet[FailedElem] => List[FailedElem] => IO[Unit] =
    failedElemStore => failedElems => IO.delay { failedElems.foreach(failedElemStore.add) }

  private val cpPersistentNodeFailures  =
    CompiledProjection.fromStream(projection1, ExecutionStrategy.PersistentSingleNode, failureStream)
  private val cpEveryNodeFailures       =
    CompiledProjection.fromStream(projection1, ExecutionStrategy.EveryNode, failureStream)
  private val cpPersistentNodeSuccesses =
    CompiledProjection.fromStream(projection1, ExecutionStrategy.PersistentSingleNode, successStream)

  test("FailedElems are saved (persistent single node)") {
    val failedElems = MutableSet.empty[FailedElem]
    for {
      projection <- Projection.apply(cpPersistentNodeFailures, IO.none, _ => IO.unit, saveFailedElems(failedElems))
      _          <- projection.executionStatus.assertEquals(ExecutionStatus.Completed).eventually
      _           = assertEquals(failedElems.size, 10)
    } yield ()
  }

  test("FailedElems are saved (every node)") {
    val failedElems = MutableSet.empty[FailedElem]
    for {
      projection <- Projection.apply(cpEveryNodeFailures, IO.none, _ => IO.unit, saveFailedElems(failedElems))
      _          <- projection.executionStatus.assertEquals(ExecutionStatus.Completed).eventually
      _           = assertEquals(failedElems.size, 10)
    } yield ()
  }

  test("Success stream saves no FailedElems") {
    val failedElems = MutableSet.empty[FailedElem]
    for {
      projection <- Projection.apply(cpPersistentNodeSuccesses, IO.none, _ => IO.unit, saveFailedElems(failedElems))
      _          <- projection.executionStatus.assertEquals(ExecutionStatus.Completed).eventually
      _           = failedElems.assertEmpty()
    } yield ()
  }

}
