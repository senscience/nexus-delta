package ai.senscience.nexus.delta.sdk.marshalling

import ai.senscience.nexus.delta.rdf.Vocabulary.{contexts, nxv, schemas}
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue.ContextRemoteIri
import ai.senscience.nexus.delta.sdk.model.source.OriginalSource
import ai.senscience.nexus.delta.sdk.model.{BaseUri, ResourceAccess, ResourceF}
import ai.senscience.nexus.delta.sourcing.model.Identity.Anonymous
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import ai.senscience.nexus.delta.sourcing.model.ResourceRef.Latest
import ai.senscience.nexus.testkit.CirceLiteral
import ai.senscience.nexus.testkit.mu.NexusSuite
import io.circe.syntax.{EncoderOps, KeyOps}
import io.circe.{Json, JsonObject}
import munit.Location

import java.time.Instant

class OriginalSourceSuite extends NexusSuite with CirceLiteral {

  private given BaseUri = BaseUri.unsafe("http://localhost", "v1")

  private val id       = nxv + "id"
  private val project  = ProjectRef.unsafe("org", "proj")
  private val resource = ResourceF(
    id,
    ResourceAccess.resource(project, schemas.resources),
    5,
    Set(nxv + "Type"),
    deprecated = false,
    Instant.EPOCH,
    Anonymous,
    Instant.EPOCH,
    Anonymous,
    Latest(schemas.resources),
    ()
  )

  private val metadataJson =
    jobj"""{
             "_constrainedBy" : "https://bluebrain.github.io/nexus/schemas/unconstrained.json",
             "_createdAt" : "1970-01-01T00:00:00Z",
             "_createdBy" : "http://localhost/v1/anonymous",
             "_deprecated" : false,
             "_project" : "org/proj",
             "_rev" : 5,
             "_self" : "http://localhost/v1/resources/org/proj/_/https:%2F%2Fbluebrain.github.io%2Fnexus%2Fschemas%2Funconstrained.json",
             "_updatedAt" : "1970-01-01T00:00:00Z",
             "_updatedBy" : "http://localhost/v1/anonymous"
           }"""

  private def assertResult(
      result: OriginalSource,
      expectedId: String,
      expectedType: String,
      expectedContext: ContextValue,
      payloadFields: (String, Json)*
  )(using Location): Unit = {
    def onObject(obj: JsonObject): Unit = {
      assertEquals(obj("@id"), Some(expectedId.asJson))
      assertEquals(obj("@type"), Some(expectedType.asJson))
      assertEquals(obj("@context"), Some(expectedContext.asJson))
      val obtainedMetadata = obj.filterKeys(_.startsWith("_"))
      assertEquals(obtainedMetadata, metadataJson)
      val payloadData      = obj.filterKeys { k => !k.startsWith("_") && !k.startsWith("@") }
      assertEquals(payloadData, JsonObject(payloadFields*))
    }

    result.asJson.arrayOrObject(
      fail("We expected an object, we got a literal"),
      _ => fail("We expected an object, we got an array"),
      onObject
    )
  }

  test("Merge metadata and source for a resource without an id, type or context") {
    val source = json"""{"source": "original payload" }"""
    assertResult(
      OriginalSource.annotated(resource, source),
      id.toString,
      resource.types.mkString,
      ContextRemoteIri(contexts.metadata),
      "source" := "original payload"
    )
  }

  test("Exclude invalid metadata at the root level") {
    val source = json"""{"source": "original payload", "_rev": 42, "_other": "xxx", "nested": { "_rev": 5} }"""
    assertResult(
      OriginalSource.annotated(resource, source),
      id.toString,
      resource.types.mkString,
      ContextRemoteIri(contexts.metadata),
      "source" := "original payload",
      "nested" := JsonObject("_rev" := 5)
    )
  }

  test("Merge metadata and source with an id and a type but no context") {
    val sourceId   = "id"
    val sourceType = "Type"
    val source     = json"""{ "@id": "$sourceId", "@type": "$sourceType", "source": "original payload" }"""
    assertResult(
      OriginalSource.annotated(resource, source),
      "id",
      "Type",
      ContextRemoteIri(contexts.metadata),
      "source" := "original payload"
    )
  }

  test("Merge metadata and source with an id, a type and a context") {
    val sourceId      = "id"
    val sourceType    = "Type"
    val sourceContext = nxv + "context"
    val source        =
      json"""{
            "@context" : "$sourceContext",
            "@id": "$sourceId",
            "@type": "$sourceType",
            "source": "original payload"
      }"""
    assertResult(
      OriginalSource.annotated(resource, source),
      "id",
      "Type",
      ContextValue(sourceContext, contexts.metadata),
      "source" := "original payload"
    )
  }

}
