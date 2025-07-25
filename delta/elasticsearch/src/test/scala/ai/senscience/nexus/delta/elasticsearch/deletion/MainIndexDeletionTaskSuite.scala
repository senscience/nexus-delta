package ai.senscience.nexus.delta.elasticsearch.deletion

import ai.senscience.nexus.delta.elasticsearch.client.{ElasticSearchAction, QueryBuilder}
import ai.senscience.nexus.delta.elasticsearch.config.MainIndexConfig
import ai.senscience.nexus.delta.elasticsearch.indexing.mainIndexingAlias
import ai.senscience.nexus.delta.elasticsearch.main.MainIndexDef
import ai.senscience.nexus.delta.elasticsearch.{ElasticSearchClientSetup, Fixtures, NexusElasticsearchSuite}
import ai.senscience.nexus.delta.sourcing.model.Identity.{Anonymous, Subject}
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.testkit.CirceLiteral
import munit.AnyFixture
import org.http4s.Query

class MainIndexDeletionTaskSuite
    extends NexusElasticsearchSuite
    with ElasticSearchClientSetup.Fixture
    with CirceLiteral
    with Fixtures {

  implicit private val subject: Subject = Anonymous

  override def munitFixtures: Seq[AnyFixture[?]] = List(esClient)

  private lazy val client = esClient()

  test("Delete all entries for a given project") {
    val mainIndexConfig = MainIndexConfig("test", "default", 1, 100)
    val index           = mainIndexConfig.index
    val projectToDelete = ProjectRef.unsafe("org", "marked-for-deletion")
    val anotherProject  = ProjectRef.unsafe("org", "another")

    val task = new MainIndexDeletionTask(client, index)

    def indexAction(id: Int, project: ProjectRef) = {
      val json = json"""{ "_project": "$project", "number": $id }"""
      ElasticSearchAction.Index(index, id.toString, Some(project.toString), json)
    }

    val bulk = List(
      indexAction(1, projectToDelete),
      indexAction(2, anotherProject),
      indexAction(3, projectToDelete),
      indexAction(4, anotherProject)
    )

    def countInIndex(project: ProjectRef) =
      for {
        query  <- task.searchByProject(project)
        result <- client.search(QueryBuilder.unsafe(query), Set(index.value), Query.empty)
      } yield result.total

    for {
      // Indexing and checking count
      mainIndexDef <- MainIndexDef(mainIndexConfig, loader)
      _            <- client.createIndex(index, Some(mainIndexDef.mapping), Some(mainIndexDef.settings))
      _            <- client.createAlias(mainIndexingAlias(index, projectToDelete))
      _            <- client.bulk(bulk)
      _            <- client.refresh(index)
      _            <- client.count(index.value).assertEquals(4L)
      // Running the task and checking the index again
      _            <- task(projectToDelete)
      _            <- client.refresh(index)
      _            <- client.count(index.value).assertEquals(2L)
      _            <- countInIndex(projectToDelete).assertEquals(0L)
      _            <- countInIndex(anotherProject).assertEquals(2L)
    } yield ()
  }

}
