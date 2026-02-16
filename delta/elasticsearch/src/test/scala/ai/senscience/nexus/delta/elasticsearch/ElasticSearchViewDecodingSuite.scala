package ai.senscience.nexus.delta.elasticsearch

import ai.senscience.nexus.delta.elasticsearch.client.{ElasticsearchMappings, ElasticsearchSettings}
import ai.senscience.nexus.delta.elasticsearch.model.ElasticSearchViewValue.{AggregateElasticSearchViewValue, IndexingElasticSearchViewValue}
import ai.senscience.nexus.delta.elasticsearch.model.permissions
import ai.senscience.nexus.delta.kernel.utils.UUIDF
import ai.senscience.nexus.delta.rdf.Vocabulary.schemas
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue.ContextObject
import ai.senscience.nexus.delta.rdf.syntax.*
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.jsonld.JsonLdRejection.{DecodingFailed, InvalidJsonLdFormat, UnexpectedId}
import ai.senscience.nexus.delta.sdk.permissions.model.Permission
import ai.senscience.nexus.delta.sdk.projects.model.{ApiMappings, ProjectContext}
import ai.senscience.nexus.delta.sdk.resolvers.ResolverContextResolution
import ai.senscience.nexus.delta.sdk.views.{PipeStep, ViewRef}
import ai.senscience.nexus.delta.sourcing.model.Tag.UserTag
import ai.senscience.nexus.delta.sourcing.model.{IriFilter, Label, ProjectRef}
import ai.senscience.nexus.delta.sourcing.stream.pipes.*
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.data.NonEmptySet
import io.circe.JsonObject
import io.circe.syntax.{EncoderOps, KeyOps}

import java.util.UUID

class ElasticSearchViewDecodingSuite extends NexusSuite with Fixtures {

  private val ref     = ProjectRef.unsafe("org", "proj")
  private val context = ProjectContext.unsafe(
    ApiMappings("_" -> schemas.resources, "resource" -> schemas.resources),
    iri"http://localhost/v1/resources/org/proj/_/",
    iri"https://schema.org/",
    enforceSchema = false
  )

  private val personType                = context.vocab / "Person"
  private val filterByPerson: IriFilter = IriFilter.restrictedTo(personType)

  private val customPermission: Permission = Permission.unsafe("custom/permission")

  private given uuidF: UUIDF                               = UUIDF.fixed(UUID.randomUUID())
  private given resolverContext: ResolverContextResolution = ResolverContextResolution(rcr)
  private given caller: Caller                             = Caller.Anonymous

  private lazy val decoder = ElasticSearchViewJsonLdSourceDecoder(uuidF, resolverContext).accepted

  // IndexingElasticSearchViewValue

  private val mapping  = ElasticsearchMappings(JsonObject("dynamic" := false))
  private val settings = ElasticsearchSettings(JsonObject("analysis" := JsonObject.empty))

  private val indexingView = IndexingElasticSearchViewValue(
    resourceTag = None,
    pipeline = IndexingElasticSearchViewValue.defaultPipeline,
    mapping = Some(mapping),
    settings = None,
    permission = permissions.query,
    context = None
  )

  test("IndexingElasticSearchView is decoded when only type and mapping are specified") {
    val source = json"""{"@type": "ElasticSearchView", "mapping": ${mapping.asJson}}"""
    decoder(ref, context, source).map { case (id, value) =>
      assertEquals(value, indexingView)
      assert(id.toString.startsWith(context.base.iri.toString))
    }
  }

  test("IndexingElasticSearchView is decoded with a context") {
    val additionalContext = JsonObject("description" := "https://schema.org/description")
    val source            =
      json"""{
               "@type": "ElasticSearchView",
               "mapping": ${mapping.asJson},
               "context": ${additionalContext.asJson},
               "pipeline": [{"name": "filterDeprecated"}]
             }"""
    decoder(ref, context, source).map { case (_, value) =>
      assertEquals(value.asIndexingValue.flatMap(_.context), Some(ContextObject(additionalContext)))
    }
  }

  test("IndexingElasticSearchView is decoded with all legacy fields") {
    val source   =
      json"""{
              "@id": "http://localhost/id",
              "@type": "ElasticSearchView",
              "name": "viewName",
              "description": "viewDescription",
              "resourceSchemas": [ "${personType.toString}" ],
              "resourceTypes": [ "${personType.toString}" ],
              "resourceTag": "release",
              "sourceAsText": false,
              "includeMetadata": false,
              "includeDeprecated": false,
              "mapping": ${mapping.asJson},
              "settings": ${settings.asJson},
              "permission": "custom/permission"
            }"""
    val expected = IndexingElasticSearchViewValue(
      name = Some("viewName"),
      description = Some("viewDescription"),
      resourceTag = Some(UserTag.unsafe("release")),
      pipeline = List(
        PipeStep(FilterBySchema(filterByPerson)),
        PipeStep(FilterByType(filterByPerson)),
        PipeStep.noConfig(FilterDeprecated.ref),
        PipeStep.noConfig(DiscardMetadata.ref),
        PipeStep.noConfig(DefaultLabelPredicates.ref)
      ),
      mapping = Some(mapping),
      settings = Some(settings),
      permission = customPermission,
      context = None
    )
    decoder(ref, context, source).map { case (id, value) =>
      assertEquals(value, expected)
      assertEquals(id, iri"http://localhost/id")
    }
  }

  test("IndexingElasticSearchView is decoded with a pipeline skipping legacy fields") {
    val source   =
      json"""{
              "@id": "http://localhost/id",
              "@type": "ElasticSearchView",
              "name": "viewName",
              "description": "viewDescription",
              "pipeline": [],
              "resourceSchemas": [ "${personType.toString}" ],
              "resourceTypes": [ "${personType.toString}" ],
              "resourceTag": "release",
              "sourceAsText": false,
              "includeMetadata": false,
              "includeDeprecated": false,
              "mapping": ${mapping.asJson},
              "settings": ${settings.asJson},
              "permission": "custom/permission"
            }"""
    val expected = IndexingElasticSearchViewValue(
      name = Some("viewName"),
      description = Some("viewDescription"),
      resourceTag = Some(UserTag.unsafe("release")),
      pipeline = List(),
      mapping = Some(mapping),
      settings = Some(settings),
      permission = customPermission,
      context = None
    )
    decoder(ref, context, source).map { case (id, value) =>
      assertEquals(value, expected)
      assertEquals(id, iri"http://localhost/id")
    }
  }

  test("IndexingElasticSearchView is decoded with a pipeline defined") {
    val source   =
      json"""{
              "@id": "http://localhost/id",
              "@type": "ElasticSearchView",
              "name": "viewName",
              "description": "viewDescription",
              "pipeline": [
                {
                  "name": "filterDeprecated"
                },
                {
                  "name": "filterByType",
                  "description": "Keep only person type",
                  "config": {
                    "types": [ "${personType.toString}" ]
                  }
                }
              ],
              "resourceSchemas": [ "${(context.vocab / "Schena").toString}" ],
              "resourceTypes": [ "${(context.vocab / "Custom").toString}" ],
              "resourceTag": "release",
              "sourceAsText": false,
              "includeMetadata": false,
              "includeDeprecated": false,
              "mapping": ${mapping.asJson},
              "settings": ${settings.asJson},
              "permission": "custom/permission"
            }"""
    val expected = IndexingElasticSearchViewValue(
      name = Some("viewName"),
      description = Some("viewDescription"),
      resourceTag = Some(UserTag.unsafe("release")),
      pipeline = List(
        PipeStep.noConfig(FilterDeprecated.ref),
        PipeStep(FilterByType(filterByPerson))
          .description("Keep only person type")
      ),
      mapping = Some(mapping),
      settings = Some(settings),
      permission = customPermission,
      context = None
    )
    decoder(ref, context, source).map { case (id, value) =>
      assertEquals(value, expected)
      assertEquals(id, iri"http://localhost/id")
    }
  }

  test("IndexingElasticSearchView is decoded when the id matches the expected id") {
    val id     = iri"http://localhost/id"
    val source = json"""{"@id": "http://localhost/id", "@type": "ElasticSearchView", "mapping": ${mapping.asJson}}"""
    decoder(ref, context, id, source).assertEquals(indexingView)
  }

  test("IndexingElasticSearchView is decoded when no id in source but one is expected") {
    val id     = iri"http://localhost/id"
    val source = json"""{"@type": "ElasticSearchView", "mapping": ${mapping.asJson}}"""
    decoder(ref, context, id, source).assertEquals(indexingView)
  }

  // IndexingElasticSearchView decoding failures

  test("IndexingElasticSearchView fails decoding when mapping is invalid") {
    val source = json"""{"@type": "ElasticSearchView", "mapping": false}"""
    decoder(ref, context, source).intercept[DecodingFailed]
  }

  test("IndexingElasticSearchView fails decoding when mapping is missing") {
    val source = json"""{"@type": "ElasticSearchView"}"""
    decoder(ref, context, source).intercept[DecodingFailed]
  }

  test("IndexingElasticSearchView fails decoding when settings are invalid") {
    val source = json"""{"@type": "ElasticSearchView", "settings": false}"""
    decoder(ref, context, source).intercept[DecodingFailed]
  }

  test("IndexingElasticSearchView fails decoding when a default field has the wrong type") {
    val source = json"""{"@type": "ElasticSearchView", "mapping": ${mapping.asJson}, "sourceAsText": 1}"""
    decoder(ref, context, source).intercept[DecodingFailed]
  }

  test("IndexingElasticSearchView fails decoding when a pipe name is missing") {
    val source =
      json"""{"@type": "ElasticSearchView", "pipeline": [{ "config": "my-config" }], "mapping": ${mapping.asJson}}"""
    decoder(ref, context, source).intercept[DecodingFailed]
  }

  test("IndexingElasticSearchView fails decoding when the provided id doesn't match the expected one") {
    val id     = iri"http://localhost/expected"
    val source =
      json"""{"@id": "http://localhost/provided", "@type": "ElasticSearchView", "mapping": ${mapping.asJson}}"""
    decoder(ref, context, id, source).intercept[UnexpectedId]
  }

  List(
    json"""{"mapping": ${mapping.asJson}}""",
    json"""{"@type": "UnknownElasticSearchView", "mapping": ${mapping.asJson}}""",
    json"""{"@type": "IndexingElasticSearchView", "mapping": ${mapping.asJson}}""",
    json"""{"@type": ["ElasticSearchView", "AggregateElasticSearchView"], "mapping": ${mapping.asJson}}"""
  ).foreach { source =>
    test(s"IndexingElasticSearchView fails decoding with no known type discriminator: ${source.noSpaces.take(60)}") {
      for {
        _ <- decoder(ref, context, source).intercept[DecodingFailed]
        _ <- decoder(ref, context, iri"http://localhost/id", source).intercept[DecodingFailed]
      } yield ()
    }
  }

  // AggregateElasticSearchViewValue

  private val viewRef1     =
    ViewRef(ProjectRef(Label.unsafe("org"), Label.unsafe("proj")), iri"http://localhost/my/proj/id")
  private val viewRef1Json = viewRef1.asJson
  private val viewRef2     =
    ViewRef(ProjectRef(Label.unsafe("org"), Label.unsafe("proj2")), iri"http://localhost/my/proj2/id")
  private val viewRef2Json = viewRef2.asJson

  test("AggregateElasticSearchView is decoded when id is provided in source") {
    val source   =
      json"""{
               "@id": "http://localhost/id",
               "@type": "AggregateElasticSearchView",
               "views": [ $viewRef1Json, $viewRef2Json ]
             }"""
    val expected = AggregateElasticSearchViewValue(NonEmptySet.of(viewRef1, viewRef2))
    decoder(ref, context, source).map { case (decodedId, value) =>
      assertEquals(value, expected)
      assertEquals(decodedId, iri"http://localhost/id")
    }
  }

  test("AggregateElasticSearchView is decoded when no id is provided in source") {
    val source   =
      json"""{
               "@type": "AggregateElasticSearchView",
               "views": [ $viewRef1Json, $viewRef1Json ]
             }"""
    val expected = AggregateElasticSearchViewValue(NonEmptySet.of(viewRef1))
    decoder(ref, context, source).map { case (decodedId, value) =>
      assertEquals(value, expected)
      assert(decodedId.toString.startsWith(context.base.iri.toString))
    }
  }

  test("AggregateElasticSearchView is decoded when id in source matches expectations") {
    val id       = iri"http://localhost/id"
    val source   =
      json"""{
               "@id": "http://localhost/id",
               "@type": "AggregateElasticSearchView",
               "views": [ $viewRef1Json ]
             }"""
    val expected = AggregateElasticSearchViewValue(NonEmptySet.of(viewRef1))
    decoder(ref, context, id, source).assertEquals(expected)
  }

  test("AggregateElasticSearchView is decoded when id is expected but source has none") {
    val id       = iri"http://localhost/id"
    val source   =
      json"""{
               "@type": "AggregateElasticSearchView",
               "views": [ $viewRef1Json ]
             }"""
    val expected = AggregateElasticSearchViewValue(NonEmptySet.of(viewRef1))
    decoder(ref, context, id, source).assertEquals(expected)
  }

  // AggregateElasticSearchView decoding failures

  test("AggregateElasticSearchView fails decoding when the view set is empty") {
    val source =
      json"""{
               "@type": "AggregateElasticSearchView",
               "views": []
             }"""
    for {
      _ <- decoder(ref, context, source).intercept[DecodingFailed]
      _ <- decoder(ref, context, iri"http://localhost/id", source).intercept[DecodingFailed]
    } yield ()
  }

  test("AggregateElasticSearchView fails decoding when the view set contains an incorrect value".ignore) {
    val source =
      json"""{
               "@type": "AggregateElasticSearchView",
               "views": [
                 {
                   "project": "org/proj",
                   "viewId": "invalid iri"
                 }
               ]
             }"""
    for {
      _ <- decoder(ref, context, source).intercept[InvalidJsonLdFormat]
      _ <- decoder(ref, context, iri"http://localhost/id", source).intercept[InvalidJsonLdFormat]
    } yield ()
  }

  List(
    json"""{"views": [ $viewRef1Json ]}""",
    json"""{"@type": "UnknownElasticSearchView", "views": [ $viewRef1Json ]}""",
    json"""{"@type": "IndexingElasticSearchView", "views": [ $viewRef1Json ]}""",
    json"""{"@type": ["ElasticSearchView", "AggregateElasticSearchView"], "views": [ $viewRef1Json ]}"""
  ).foreach { source =>
    test(s"AggregateElasticSearchView fails decoding with no known type discriminator: ${source.noSpaces.take(60)}") {
      for {
        _ <- decoder(ref, context, source).intercept[DecodingFailed]
        _ <- decoder(ref, context, iri"http://localhost/id", source).intercept[DecodingFailed]
      } yield ()
    }
  }
}
