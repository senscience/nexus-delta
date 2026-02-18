package ai.senscience.nexus.delta.plugins.blazegraph

import ai.senscience.nexus.delta.kernel.utils.UUIDF
import ai.senscience.nexus.delta.plugins.blazegraph.model.BlazegraphViewValue.{AggregateBlazegraphViewValue, IndexingBlazegraphViewValue}
import ai.senscience.nexus.delta.plugins.blazegraph.model.{contexts, BlazegraphViewValue}
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.rdf.jsonld.decoder.Configuration
import ai.senscience.nexus.delta.rdf.syntax.*
import ai.senscience.nexus.delta.sdk.jsonld.JsonLdRejection.{DecodingFailed, InvalidJsonLdFormat, UnexpectedId}
import ai.senscience.nexus.delta.sdk.jsonld.JsonLdSourceProcessor.JsonLdSourceDecoder
import ai.senscience.nexus.delta.sdk.permissions.model.Permission
import ai.senscience.nexus.delta.sdk.projects.model.{ApiMappings, ProjectContext}
import ai.senscience.nexus.delta.sdk.views.ViewRef
import ai.senscience.nexus.delta.sourcing.model.Tag.UserTag
import ai.senscience.nexus.delta.sourcing.model.{IriFilter, Label, ProjectRef}
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.data.NonEmptySet
import cats.effect.IO
import cats.syntax.all.*

import java.util.UUID

class BlazegraphViewDecodingSuite extends NexusSuite with Fixtures {

  private val context = ProjectContext.unsafe(
    ApiMappings.empty,
    nxv.base,
    nxv.base,
    enforceSchema = false
  )

  private given uuidF: UUIDF          = UUIDF.fixed(UUID.randomUUID())
  private given config: Configuration = BlazegraphDecoderConfiguration.apply.accepted
  private val decoder                 = new JsonLdSourceDecoder[BlazegraphViewValue](contexts.blazegraph, uuidF)

  // IndexingBlazegraphValue decoding

  test("Decode IndexingBlazegraphViewValue when only its type is specified") {
    val source = json"""{"@type": "SparqlView"}"""
    decoder(context, source).map { case (id, value) =>
      assertEquals(value, IndexingBlazegraphViewValue())
      assert(id.toString.startsWith(context.base.iri.toString))
    }
  }

  private val personIri = (context.vocab / "Person").toString

  test("Decode IndexingBlazegraphViewValue when all fields are specified") {
    val source   =
      json"""{
              "@id": "http://localhost/id",
              "@type": "SparqlView",
              "name": "viewName",
              "description": "viewDescription",
              "resourceSchemas": [ "$personIri" ],
              "resourceTypes": [ "$personIri" ],
              "resourceTag": "release",
              "includeMetadata": false,
              "includeDeprecated": false,
              "permission": "custom/permission"
            }"""
    val expected = IndexingBlazegraphViewValue(
      name = Some("viewName"),
      description = Some("viewDescription"),
      resourceSchemas = IriFilter.restrictedTo(context.vocab / "Person"),
      resourceTypes = IriFilter.restrictedTo(context.vocab / "Person"),
      resourceTag = Some(UserTag.unsafe("release")),
      includeMetadata = false,
      includeDeprecated = false,
      permission = Permission.unsafe("custom/permission")
    )
    decoder(context, source).map { case (id, value) =>
      assertEquals(value, expected)
      assertEquals(id, iri"http://localhost/id")
    }
  }

  test("Fail decoding IndexingBlazegraphViewValue when the provided id does not match") {
    val id     = iri"http://localhost/expected"
    val source = json"""{"@id": "http://localhost/provided", "@type": "SparqlView"}"""
    decoder(context, id, source).intercept[UnexpectedId]
  }

  test("Fail decoding when there's no known type discriminator") {
    val sources = List(
      json"""{}""",
      json"""{"@type": "UnknownSparqlView"}"""
    )
    sources.traverse { source =>
      decoder(context, iri"http://localhost/id", source).intercept[DecodingFailed]
    }
  }

  // AggregateBlazegraphViewValue decoding

  private val viewRef1     =
    ViewRef(ProjectRef(Label.unsafe("org"), Label.unsafe("proj")), iri"http://localhost/my/proj/id")
  private val viewRef1Json = json"""{
                            "project": "org/proj",
                            "viewId": "http://localhost/my/proj/id"
                          }"""
  private val viewRef2     =
    ViewRef(ProjectRef(Label.unsafe("org"), Label.unsafe("proj2")), iri"http://localhost/my/proj2/id")
  private val viewRef2Json = json"""{
                            "project": "org/proj2",
                            "viewId": "http://localhost/my/proj2/id"
                          }"""

  test("Decode AggregateBlazegraphViewValue when the id is provided in the source") {
    val source   =
      json"""{
               "@id": "http://localhost/id",
               "@type": "AggregateSparqlView",
               "views": [ $viewRef1Json, $viewRef2Json ]
             }"""
    val expected = AggregateBlazegraphViewValue(None, None, NonEmptySet.of(viewRef1, viewRef2))
    decoder(context, source).map { case (decodedId, value) =>
      assertEquals(value, expected)
      assertEquals(decodedId, iri"http://localhost/id")
    }
  }

  test("Decode AggregateBlazegraphViewValue when an id is not provided in the source") {
    val source   =
      json"""{
               "@type": "AggregateSparqlView",
               "views": [ $viewRef1Json, $viewRef1Json ]
             }"""
    val expected = AggregateBlazegraphViewValue(None, None, NonEmptySet.of(viewRef1))
    decoder(context, source).map { case (decodedId, value) =>
      assertEquals(value, expected)
      assert(decodedId.toString.startsWith(context.base.iri.toString))
    }
  }

  test("Decode AggregateBlazegraphViewValue when the id matches expectations") {
    val id       = iri"http://localhost/id"
    val source   =
      json"""{
               "@id": "http://localhost/id",
               "@type": "AggregateSparqlView",
               "views": [ $viewRef1Json ]
             }"""
    val expected = AggregateBlazegraphViewValue(None, None, NonEmptySet.of(viewRef1))
    decoder(context, id, source).assertEquals(expected)
  }

  test("Decode AggregateBlazegraphViewValue when all fields are specified") {
    val source   =
      json"""{
               "@id": "http://localhost/id",
               "@type": "AggregateSparqlView",
               "name": "viewName",
               "description": "viewDescription",
               "views": [ $viewRef1Json, $viewRef2Json ]
             }"""
    val expected =
      AggregateBlazegraphViewValue(Some("viewName"), Some("viewDescription"), NonEmptySet.of(viewRef1, viewRef2))
    decoder(context, source).map { case (decodedId, value) =>
      assertEquals(value, expected)
      assertEquals(decodedId, iri"http://localhost/id")
    }
  }

  test("Fail decoding AggregateBlazegraphViewValue when the view set is empty") {
    val source =
      json"""{
               "@type": "AggregateSparqlView",
               "views": []
             }"""
    decoder(context, source).intercept[DecodingFailed] >>
      decoder(context, iri"http://localhost/id", source).intercept[DecodingFailed]
  }

  test("Fail decoding AggregateBlazegraphViewValue when the view set contains an incorrect value".ignore) {
    val source =
      json"""{
               "@type": "AggregateSparqlView",
               "views": [
                 {
                   "project": "org/proj",
                   "viewId": "invalid iri"
                 }
               ]
             }"""
    decoder(context, source).intercept[InvalidJsonLdFormat] >>
      decoder(context, iri"http://localhost/id", source).intercept[InvalidJsonLdFormat]
  }

  test("Fail decoding AggregateBlazegraphViewValue when there's no known type discriminator") {
    val sources = List(
      json"""{"views": [ $viewRef1Json ]}""",
      json"""{"@type": "UnknownSparqlView", "views": [ $viewRef1Json ]}"""
    )
    sources.traverse_ { source =>
      decoder(context, source).intercept[DecodingFailed] >>
        decoder(context, iri"http://localhost/id", source).intercept[DecodingFailed]
    }
  }
}
