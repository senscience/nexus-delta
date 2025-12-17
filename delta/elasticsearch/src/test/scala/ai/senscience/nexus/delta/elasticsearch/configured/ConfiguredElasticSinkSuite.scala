package ai.senscience.nexus.delta.elasticsearch.configured

import ai.senscience.nexus.delta.elasticsearch.client.{IndexLabel, QueryBuilder, Refresh}
import ai.senscience.nexus.delta.elasticsearch.model.ElasticsearchIndexDef
import ai.senscience.nexus.delta.elasticsearch.{ElasticSearchClientSetup, NexusElasticsearchSuite}
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.sourcing.model.{EntityType, ProjectRef}
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.stream.Elem.{DroppedElem, FailedElem, SuccessElem}
import ai.senscience.nexus.delta.sourcing.stream.FailureReason
import ai.senscience.nexus.delta.sourcing.stream.config.BatchConfig
import cats.syntax.all.*
import fs2.Chunk
import munit.{AnyFixture, Location}
import org.http4s.Query

import java.time.Instant
import scala.concurrent.duration.DurationInt

class ConfiguredElasticSinkSuite extends NexusElasticsearchSuite with ElasticSearchClientSetup.Fixture {

  override def munitFixtures: Seq[AnyFixture[?]] = List(esClient)

  private val indexDef = ElasticsearchIndexDef.empty

  private lazy val client = esClient()

  private val authorType      = nxv + "Author"
  private val publicationType = nxv + "Publication"

  private val indexAuthors = IndexLabel.unsafe("authors")
  private val indexPapers  = IndexLabel.unsafe("papers")

  private lazy val configuredSink = new ConfiguredElasticSink(
    client,
    BatchConfig(2, 50.millis),
    Map(
      authorType      -> indexAuthors,
      publicationType -> indexPapers
    ),
    Refresh.True
  )

  private val entityType = EntityType("entity")

  private val project = ProjectRef.unsafe("senscience", "my-paper")

  private val alice = (nxv + "alice", ConfiguredIndexDocument(Set(authorType), json"""{"name": "Alice", "age": 25 }"""))
  private val bob   = (nxv + "bob", ConfiguredIndexDocument(Set(authorType), json"""{"name": "Bob", "age": 32 }"""))
  private val brian = (nxv + "brian", ConfiguredIndexDocument(Set(authorType), json"""{"name": "Brian", "age": 19 }"""))

  private val paper1 = (nxv + "paper1", ConfiguredIndexDocument(Set(publicationType), json"""{"title": "Paper 1" }"""))
  private val paper2 = (nxv + "paper2", ConfiguredIndexDocument(Set(publicationType), json"""{"title": "Paper 2" }"""))

  private val allDocs = List(alice, bob, brian, paper1, paper2)

  private val rev = 1

  private def asChunk(values: (Iri, ConfiguredIndexDocument)*) =
    Chunk.from(values).zipWithIndex.map { case ((id, doc), index) =>
      SuccessElem(entityType, id, project, Instant.EPOCH, Offset.at(index.toLong + 1), doc, rev)
    }

  private def dropped(id: Iri, offset: Offset) =
    DroppedElem(entityType, id, project, Instant.EPOCH, offset, rev)

  private val failed =
    FailedElem(entityType, nxv + "fail", project, Instant.EPOCH, Offset.at(1L), new IllegalArgumentException("Boom"), 1)

  private def assertSearch(index: IndexLabel, expected: (Iri, ConfiguredIndexDocument)*)(using Location) = {
    val expectedSources = expected.toSet.flatMap(_._2.value.asObject)
    client
      .search(QueryBuilder.empty, Set(index.value), Query.empty)
      .map(_.sources.toSet)
      .assertEquals(expectedSources)
  }

  test("Create the index") {
    (indexAuthors, indexPapers).traverse { index =>
      client.createIndex(index, indexDef).assertEquals(true)
    }
  }

  test("Index a chunk of documents and retrieve them") {
    val chunk = asChunk(allDocs*)

    for {
      // First index successfully everything
      _           <- configuredSink(chunk).assertEquals(chunk.map(_.void))
      _           <- assertSearch(indexAuthors, alice, bob, brian)
      _           <- assertSearch(indexPapers, paper1, paper2)
      // Deleting alice
      deleteAlice  = Chunk.singleton(dropped(alice._1, Offset.at(allDocs.size + 1)))
      _           <- configuredSink(deleteAlice).assertEquals(deleteAlice.map(_.void))
      _           <- assertSearch(indexAuthors, bob, brian)
      _           <- assertSearch(indexPapers, paper1, paper2)
      // Changing brian type
      brianAsPaper = brian.copy(_2 = ConfiguredIndexDocument(Set(publicationType), json"""{"title": "Brian 1" }"""))
      _           <- configuredSink(asChunk(brianAsPaper))
      _           <- assertSearch(indexAuthors, bob)
      _           <- assertSearch(indexPapers, paper1, paper2, brianAsPaper)
      // Invalid author
      invalid      = (nxv + "xxx", ConfiguredIndexDocument(Set(authorType), json"""{"name": 24601, "age": "xxx" }"""))
      sinkResult  <- configuredSink(asChunk(invalid) ++ Chunk.singleton(failed))
      _            = assertEquals(
                       sinkResult.size,
                       2,
                       "2 elements were submitted to the sink, we expect 2 elements in the result chunk."
                     )
      _            = sinkResult.head match {
                       case Some(f: FailedElem) =>
                         f.throwable match {
                           case reason: FailureReason =>
                             assertEquals(reason.`type`, "IndexingFailure")
                           case t                     => fail(s"An indexing failure was expected, got '$t'", t)
                         }
                       case other               => fail(s"A failed elem was expected, got '$other'")
                     }
      _            = assertEquals(sinkResult.last, Some(failed))
    } yield ()

  }
}
