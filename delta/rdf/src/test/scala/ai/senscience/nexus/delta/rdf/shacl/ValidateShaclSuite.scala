package ai.senscience.nexus.delta.rdf.shacl

import ai.senscience.nexus.delta.rdf.Vocabulary.contexts
import ai.senscience.nexus.delta.rdf.jsonld.ExpandedJsonLd
import ai.senscience.nexus.delta.rdf.jsonld.api.{JsonLdApi, TitaniumJsonLdApi}
import ai.senscience.nexus.delta.rdf.jsonld.context.JsonLdContext.keywords
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.syntax.*
import ai.senscience.nexus.testkit.mu.NexusSuite
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.apache.jena.shacl.Shapes

class ValidateShaclSuite extends NexusSuite {

  private given JsonLdApi = TitaniumJsonLdApi.strict

  private val schema           = jsonContentOf("shacl/schema.json")
  private val data             = jsonContentOf("shacl/resource.json")
  private val shaclResolvedCtx = jsonContentOf("contexts/shacl.json").topContextValueOrEmpty
  
  private given rcr: RemoteContextResolution = RemoteContextResolution.fixed(contexts.shacl -> shaclResolvedCtx)

  private val shaclValidation = ValidateShacl(rcr).accepted

  private val schemaGraph = toGraph(schema).accepted
  private val schemaShapes = Shapes.parse(schemaGraph.value.getDefaultGraph )
  private val dataGraph   = toGraph(data).accepted

  private def toGraph(json: Json) = ExpandedJsonLd(json).flatMap(_.toGraph)

  test("Validate data from schema model") {
    shaclValidation(dataGraph, schemaShapes).assert(_.conformsWithTargetedNodes)
  }

  test("Fail validating data if not matching nodes") {
    val dataChangedType = data.replace(keywords.tpe -> "Custom", "Other")
    for {
      resourceGraph <- toGraph(dataChangedType)
      _             <- shaclValidation(resourceGraph, schemaShapes).assert { report =>
                         !report.conformsWithTargetedNodes && report.targetedNodes == 0
                       }
    } yield ()
  }

  test("Fail validating data if wrong field type") {
    val dataInvalidNumber = data.replace("number" -> 24, "Other")
    val expectedJson    = jsonContentOf("shacl/failed_number.json")
    for {
      wrongGraph <- toGraph(dataInvalidNumber)
      report          <- shaclValidation(wrongGraph, schemaShapes)
    } yield {
      assert(!report.conformsWithTargetedNodes, "Validation should have failed")
      assertEquals(report.asJson, expectedJson)
    }
  }

  test("Validate shapes") {
    shaclValidation(schemaGraph).assert(_.conformsWithTargetedNodes)
  }

  test("Fail validating shapes if no property is defined") {
    val wrongSchema = schema.mapAllKeys("property", _ => Json.obj())

    toGraph(wrongSchema).flatMap { wrongGraph =>
      shaclValidation(wrongGraph).assert(
        _.conformsWithTargetedNodes == false,
        "Validation should fail as property requires at least one element"
      )
    }
  }

}
