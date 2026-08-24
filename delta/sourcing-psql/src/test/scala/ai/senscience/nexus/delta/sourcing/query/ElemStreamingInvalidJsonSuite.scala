package ai.senscience.nexus.delta.sourcing.query

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sourcing.config.ElemQueryConfig.StopConfig
import ai.senscience.nexus.delta.sourcing.model.{ProjectRef, Tag}
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.postgres.Doobie
import ai.senscience.nexus.delta.sourcing.stream.Elem
import ai.senscience.nexus.delta.sourcing.stream.Elem.{DroppedElem, FailedElem, SuccessElem}
import ai.senscience.nexus.delta.sourcing.{PullRequest, Scope}
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.effect.IO
import munit.AnyFixture
import org.typelevel.doobie.postgres.implicits.*
import org.typelevel.doobie.syntax.all.*

import java.time.Instant
import scala.concurrent.duration.DurationInt

/**
  * A state which cannot be parsed must not interrupt the streams consuming it, otherwise the projections indexing it
  * keep crashing and restarting on the very same offset.
  */
class ElemStreamingInvalidJsonSuite extends NexusSuite with Doobie.Fixture {

  override def munitFixtures: Seq[AnyFixture[?]] = List(doobie)

  private lazy val xas = doobie()

  private lazy val elemStreaming =
    ElemStreaming.stopping(xas, EntityTypeFilter.include(PullRequest.entityType), StopConfig(10, 50.millis))

  private val project   = ProjectRef.unsafe("org", "proj")
  private val invalidId = nxv + "invalid"
  private val validId   = nxv + "valid"

  /**
    * Postgres accepts this number in a jsonb column and renders it back in plain notation, which exceeds the limit for
    * the number of digits enforced by the json parser.
    */
  private val tooManyDigits = s"0.${"0" * 400}1"

  private def insert(id: Iri, value: String) =
    sql"""INSERT INTO public.scoped_states (type, org, project, id, tag, rev, value, deprecated, instant)
         |VALUES (${PullRequest.entityType.value}, ${project.organization.value}, ${project.project.value},
         |        ${id.toString}, ${Tag.latest.value}, 1, $value::jsonb, false, ${Instant.EPOCH})
         |""".stripMargin.update.run.transact(xas.write)

  test("Setting up the state log") {
    insert(invalidId, s"""{"id": "$invalidId", "mean": $tooManyDigits}""") >>
      insert(validId, s"""{"id": "$validId"}""")
  }

  test("Return an unparseable state as a failed elem and keep streaming the following ones") {
    elemStreaming(Scope(project), Offset.Start, SelectFilter.latest, (_, json) => IO.pure(json)).compile.toList
      .map(_.map(kindAndId))
      .assertEquals(List("failed" -> invalidId, "success" -> validId))
  }

  private def kindAndId(elem: Elem[?]) = elem match {
    case e: FailedElem     => "failed"  -> e.id
    case e: SuccessElem[?] => "success" -> e.id
    case e: DroppedElem    => "dropped" -> e.id
  }
}
