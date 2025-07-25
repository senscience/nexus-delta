package ai.senscience.nexus.delta.elasticsearch.model

import ai.senscience.nexus.delta.elasticsearch.Fixtures
import ai.senscience.nexus.delta.elasticsearch.model.ElasticSearchView.{AggregateElasticSearchView, IndexingElasticSearchView}
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue.ContextObject
import ai.senscience.nexus.delta.sdk.permissions.model.Permission
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sdk.views.{PipeStep, ViewRef}
import ai.senscience.nexus.delta.sourcing.model.Tag.UserTag
import ai.senscience.nexus.delta.sourcing.model.{IriFilter, ProjectRef, Tags}
import ai.senscience.nexus.delta.sourcing.stream.pipes.*
import ai.senscience.nexus.testkit.scalatest.ce.CatsEffectSpec
import ai.senscience.nexus.testkit.{CirceEq, CirceLiteral}
import cats.data.NonEmptySet

import java.util.UUID

class ElasticSearchViewSpec extends CatsEffectSpec with CirceLiteral with CirceEq with Fixtures {

  private val id      = nxv + "myview"
  private val project = ProjectRef.unsafe("org", "project")
  private val tagsMap = Tags(UserTag.unsafe("tag") -> 1)
  private val source  = json"""{"source": "value"}"""
  private val perm    = Permission.unsafe("views/query")

  "An IndexingElasticSearchView" should {
    val uuid                                                      = UUID.fromString("f85d862a-9ec0-4b9a-8aed-2938d7ca9981")
    def indexingView(pipeline: List[PipeStep]): ElasticSearchView = IndexingElasticSearchView(
      id,
      Some("viewName"),
      Some("viewDescription"),
      project,
      uuid,
      Some(UserTag.unsafe("mytag")),
      pipeline,
      jobj"""{"properties": {"@type": {"type": "keyword"}, "@id": {"type": "keyword"} } }""",
      jobj"""{"analysis": {"analyzer": {"nexus": {} } } }""",
      context = Some(ContextObject(jobj"""{"@vocab": "http://schema.org/"}""")),
      perm,
      tagsMap,
      source
    )
    "be converted to compacted Json-LD" in {
      forAll(
        List(
          PipeStep(FilterBySchema(IriFilter.restrictedTo(nxv.Schema))),
          PipeStep(FilterByType(IriFilter.restrictedTo(nxv + "Morphology"))),
          PipeStep.noConfig(SourceAsText.ref).description("Formatting source as text")
        )        -> "jsonld/indexing-view-compacted-1.json" ::
          List(
            PipeStep.noConfig(FilterDeprecated.ref),
            PipeStep.noConfig(DiscardMetadata.ref)
          )      -> "jsonld/indexing-view-compacted-2.json" ::
          List() -> "jsonld/indexing-view-compacted-3.json" :: Nil
      ) { case (pipeline, expected) =>
        indexingView(pipeline).toCompactedJsonLd.accepted.json shouldEqual jsonContentOf(expected)
      }
    }
    "be converted to expanded Json-LD" in {
      indexingView(
        List(
          PipeStep(FilterBySchema(IriFilter.restrictedTo(nxv.Schema))),
          PipeStep(FilterByType(IriFilter.restrictedTo(nxv + "Morphology"))),
          PipeStep.noConfig(SourceAsText.ref).description("Formatting source as text")
        )
      ).toExpandedJsonLd.accepted.json shouldEqual jsonContentOf("jsonld/indexing-view-expanded.json")
    }
  }
  "An AggregateElasticSearchView" should {
    val view: ElasticSearchView = AggregateElasticSearchView(
      id,
      Some("viewName"),
      Some("viewDescription"),
      project,
      NonEmptySet.of(ViewRef(project, nxv + "view1"), ViewRef(project, nxv + "view2")),
      tagsMap,
      source
    )
    "be converted to compacted Json-LD" in {
      view.toCompactedJsonLd.accepted.json should equalIgnoreArrayOrder(jsonContentOf("jsonld/agg-view-compacted.json"))
    }
    "be converted to expanded Json-LD" in {
      view.toExpandedJsonLd.accepted.json should equalIgnoreArrayOrder(jsonContentOf("jsonld/agg-view-expanded.json"))
    }
  }

}
