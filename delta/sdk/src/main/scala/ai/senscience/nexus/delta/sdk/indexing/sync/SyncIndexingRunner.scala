package ai.senscience.nexus.delta.sdk.indexing.sync

import ai.senscience.nexus.delta.sourcing.stream.Elem.FailedElem
import ai.senscience.nexus.delta.sourcing.stream.config.BatchConfig
import ai.senscience.nexus.delta.sourcing.stream.{CompiledProjection, Projection}
import cats.effect.{IO, Ref}
import fs2.Stream

import scala.concurrent.duration.FiniteDuration

object SyncIndexingRunner {

  private type ErrorList = List[Throwable]

  final private class IndexingActionContext(value: Ref[IO, ErrorList]) {
    def addError(error: Throwable): IO[Unit] = value.update(_.+:(error))

    def addErrors(failed: List[FailedElem]): IO[Unit] = value.update(_ ++ failed.map(_.throwable))

    def get: IO[ErrorList] = value.get
  }

  private object IndexingActionContext {
    def apply(): IO[IndexingActionContext] =
      Ref.of[IO, ErrorList](List.empty).map(new IndexingActionContext(_))
  }

  private given BatchConfig = BatchConfig.individual

  def apply(projections: Stream[IO, CompiledProjection], timeout: FiniteDuration): IO[SyncIndexingOutcome] = {
    for {
      errors  <- IndexingActionContext()
      _       <- projections.evalMap(runProjection(_, errors, timeout)).compile.drain
      outcome <- errors.get.map {
                   case l if l.isEmpty => SyncIndexingOutcome.Success
                   case l              => SyncIndexingOutcome.Failed(l)
                 }
    } yield outcome
  }

  private def runProjection(compiled: CompiledProjection, errors: IndexingActionContext, timeout: FiniteDuration) =
    Projection(compiled, IO.none, _ => IO.unit, errors.addErrors)
      .flatMap { projection =>
        // We wait the projection if it has not complete yet
        projection.waitForCompletion(timeout) >> projection.stop()
      }
      .handleErrorWith {
        errors.addError
      }
}
