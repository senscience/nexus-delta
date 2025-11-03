package ai.senscience.nexus.delta.sourcing

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sourcing.Arithmetic.ArithmeticCommand.{Add, Boom, Never, Subtract}
import ai.senscience.nexus.delta.sourcing.Arithmetic.ArithmeticEvent.{Minus, Plus}
import ai.senscience.nexus.delta.sourcing.Arithmetic.ArithmeticRejection.{AlreadyExists, NegativeTotal, NotFound, RevisionNotFound}
import ai.senscience.nexus.delta.sourcing.Arithmetic.{ArithmeticCommand, ArithmeticEvent, ArithmeticRejection, Total}
import ai.senscience.nexus.delta.sourcing.EvaluationError.EvaluationTimeout
import ai.senscience.nexus.delta.sourcing.config.QueryConfig
import ai.senscience.nexus.delta.sourcing.event.GlobalEventStore
import ai.senscience.nexus.delta.sourcing.implicits.*
import ai.senscience.nexus.delta.sourcing.postgres.Doobie
import ai.senscience.nexus.delta.sourcing.query.RefreshStrategy
import ai.senscience.nexus.delta.sourcing.state.{GlobalStateStore, ProjectionStateSave}
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.syntax.all.*
import doobie.syntax.all.*
import munit.AnyFixture

import scala.concurrent.duration.*

class GlobalEventLogSuite extends NexusSuite with Doobie.Fixture {

  override def munitFixtures: Seq[AnyFixture[?]] = List(doobie)

  private lazy val xas = doobie()

  private val queryConfig = QueryConfig(10, RefreshStrategy.Delay(500.millis))

  private lazy val eventStore = GlobalEventStore[Iri, ArithmeticEvent](
    Arithmetic.entityType,
    ArithmeticEvent.serializer,
    queryConfig,
    xas
  )

  private lazy val stateStore = GlobalStateStore[Iri, Total](
    Arithmetic.entityType,
    Total.serializer,
    queryConfig,
    xas
  )

  private val maxDuration = 100.millis

  private def projectionSave = ProjectionStateSave[Iri, Total](
    (id: Iri, total: Total) => sql"""| CREATE TABLE IF NOT EXISTS public.test_total_projection(
            |    id        text            NOT NULL,
            |    total     bigint          NOT NULL
            | );
            | INSERT INTO public.test_total_projection VALUES ($id, ${total.value}) ;
            |""".stripMargin.update.run.void,
    _ => sql"""DROP TABLE IF EXISTS public.test_total_projection;""".update.run.void
  )

  private lazy val eventLog: GlobalEventLog[Iri, Total, ArithmeticCommand, ArithmeticEvent, ArithmeticRejection] =
    GlobalEventLog(
      eventStore,
      stateStore,
      Arithmetic.stateMachine,
      AlreadyExists(_, _),
      projectionSave,
      maxDuration,
      xas
    )

  private def getProjectionTotal(id: Iri) =
    sql"""SELECT total from public.test_total_projection where id = $id""".query[Int].unique.transact(xas.read)

  private val plus2  = Plus(1, 2)
  private val plus3  = Plus(2, 3)
  private val minus4 = Minus(3, 4)

  private val total1 = Total(1, 2)
  private val total2 = Total(2, 5)
  private val total3 = Total(3, 1)

  private val id = nxv + "id"

  test("Raise an error with a non-existent " + id) {
    eventLog.stateOr(nxv + "xxx", NotFound).interceptEquals(NotFound)
  }

  test("Evaluate successfully a command and store both event and state for an initial state") {
    for {
      _ <- eventLog.evaluate(id, Add(2)).assertEquals((plus2, total1))
      _ <- eventStore.history(id).assert(plus2)
      _ <- eventLog.stateOr(id, NotFound).assertEquals(total1)
      _ <- getProjectionTotal(id).assertEquals(total1.value)
    } yield ()
  }

  test("Evaluate successfully another and store both event and state for an initial state") {
    for {
      _ <- eventLog.evaluate(id, Add(3)).assertEquals((plus3, total2))
      _ <- eventStore.history(id).assert(plus2, plus3)
      _ <- eventLog.stateOr(id, NotFound).assertEquals(total2)
    } yield ()
  }

  test("Reject a command and persist nothing") {
    for {
      _ <- eventLog.evaluate(id, Subtract(8)).interceptEquals(NegativeTotal(-3))
      _ <- eventStore.history(id).assert(plus2, plus3)
      _ <- eventLog.stateOr(id, NotFound).assertEquals(total2)
    } yield ()
  }

  test("Raise an error and persist nothing") {
    val boom = Boom("fail")
    for {
      _ <- eventLog.evaluate(id, boom).intercept[RuntimeException]
      _ <- eventStore.history(id).assert(plus2, plus3)
      _ <- eventLog.stateOr(id, NotFound).assertEquals(total2)
    } yield ()
  }

  test("Get a timeout and persist nothing") {
    for {
      _ <- eventLog.evaluate(id, Never).interceptEquals(EvaluationTimeout(Never, maxDuration))
      _ <- eventStore.history(id).assert(plus2, plus3)
      _ <- eventLog.stateOr(id, NotFound).assertEquals(total2)
    } yield ()
  }

  test("Dry run successfully a command without persisting anything") {
    for {
      _ <- eventLog.dryRun(id, Subtract(4)).assertEquals((minus4, total3))
      _ <- eventStore.history(id).assert(plus2, plus3)
      _ <- eventLog.stateOr(id, NotFound).assertEquals(total2)
    } yield ()
  }

  test("Get state at the specified revision") {
    eventLog.stateOr(id, 1, NotFound, RevisionNotFound(_, _)).assertEquals(total1)
  }

  test("Raise an error with a non-existent " + id) {
    eventLog.stateOr(nxv + "xxx", 1, NotFound, RevisionNotFound(_, _)).interceptEquals(NotFound)
  }

  test("Raise an error when providing a nonexistent revision") {
    eventLog.stateOr(id, 10, NotFound, RevisionNotFound(_, _)).interceptEquals(RevisionNotFound(10, 2))
  }

}
