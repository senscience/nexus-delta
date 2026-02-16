package ai.senscience.nexus.delta.elasticsearch.model

import ai.senscience.nexus.delta.elasticsearch.Fixtures
import ai.senscience.nexus.delta.elasticsearch.client.{ElasticsearchMappings, ElasticsearchSettings}
import ai.senscience.nexus.delta.elasticsearch.model.ElasticSearchView.{AggregateElasticSearchView, IndexingElasticSearchView}
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.rdf.jsonld.api.{JsonLdApi, TitaniumJsonLdApi}
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue.ContextObject
import ai.senscience.nexus.delta.sdk.permissions.model.Permission
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sdk.views.{PipeStep, ViewRef}
import ai.senscience.nexus.delta.sourcing.model.Tag.UserTag
import ai.senscience.nexus.delta.sourcing.model.{IriFilter, ProjectRef, Tags}
import ai.senscience.nexus.delta.sourcing.stream.pipes.*
import ai.senscience.nexus.testkit.mu.NexusSuite
import ai.senscience.nexus.testkit.mu.JsonAssertions
import cats.data.NonEmptySet

import java.util.UUID

class ElasticSearchViewSuite extends NexusSuite with JsonAssertions with Fixtures {

  private given JsonLdApi = TitaniumJsonLdApi.strict
  private val id          = nxv + "myview"
  private val project     = ProjectRef.unsafe("org", "project")
  private val tagsMap     = Tags(UserTag.unsafe("tag") -> 1)
  private val source      = json"""{"source": "value"}"""
  private val perm        = Permission.unsafe("views/query")

  private val uuid = UUID.fromString("f85d862a-9ec0-4b9a-8aed-2938d7ca9981")

  private def indexingView(pipeline: List[PipeStep]): ElasticSearchView = IndexingElasticSearchView(
    id,
    Some("viewName"),
    Some("viewDescription"),
    project,
    uuid,
    Some(UserTag.unsafe("mytag")),
    pipeline,
    ElasticsearchMappings(jobj"""{"properties": {"@type": {"type": "keyword"}, "@id": {"type": "keyword"} } }"""),
    Some(ElasticsearchSettings(jobj"""{"analysis": {"analyzer": {"nexus": {} } } }""")),
    context = Some(ContextObject(jobj"""{"@vocab": "https://schema.org/"}""")),
    perm,
    tagsMap,
    source
  )

  // IndexingElasticSearchView
  private val pipeline1 = List(
    PipeStep(FilterBySchema(IriFilter.restrictedTo(nxv.Schema))),
    PipeStep(FilterByType(IriFilter.restrictedTo(nxv + "Morphology"))),
    PipeStep.noConfig(SourceAsText.ref).description("Formatting source as text")
  )

  private val pipeline2 = List(PipeStep.noConfig(FilterDeprecated.ref), PipeStep.noConfig(DiscardMetadata.ref))

  List(
    pipeline1 -> "jsonld/indexing-view-compacted-1.json",
    pipeline2 -> "jsonld/indexing-view-compacted-2.json",
    List()    -> "jsonld/indexing-view-compacted-3.json"
  ).zipWithIndex.foreach { case ((pipeline, expectedFile), idx) =>
    test(s"IndexingElasticSearchView is converted to compacted Json-LD (case ${idx + 1})") {
      indexingView(pipeline).toCompactedJsonLd.map(_.json).assertEquals(jsonContentOf(expectedFile))
    }
  }

  test("IndexingElasticSearchView is converted to expanded Json-LD") {
    indexingView(pipeline1).toExpandedJsonLd
      .map(_.json)
      .assertEquals(jsonContentOf("jsonld/indexing-view-expanded.json"))
  }

  // AggregateElasticSearchView
  private val views                      = NonEmptySet.of(ViewRef(project, nxv + "view1"), ViewRef(project, nxv + "view2"))
  private val aggView: ElasticSearchView =
    AggregateElasticSearchView(id, Some("viewName"), Some("viewDescription"), project, views, tagsMap, source)

  test("AggregateElasticSearchView is converted to compacted Json-LD") {
    aggView.toCompactedJsonLd.map { result =>
      result.json.equalsIgnoreArrayOrder(jsonContentOf("jsonld/agg-view-compacted.json"))
    }
  }

  test("AggregateElasticSearchView is converted to expanded Json-LD") {
    aggView.toExpandedJsonLd.map { result =>
      result.json.equalsIgnoreArrayOrder(jsonContentOf("jsonld/agg-view-expanded.json"))
    }
  }
}
