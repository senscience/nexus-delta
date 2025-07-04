package ai.senscience.nexus.delta.sdk.schemas

import ai.senscience.nexus.delta.rdf.jsonld.ExpandedJsonLd
import ai.senscience.nexus.delta.sdk.Resolve
import ai.senscience.nexus.delta.sdk.generators.ResourceGen
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.resolvers.model.ResourceResolutionReport
import ai.senscience.nexus.delta.sdk.resources.model.Resource
import ai.senscience.nexus.delta.sdk.schemas.model.Schema
import ai.senscience.nexus.delta.sdk.schemas.model.SchemaRejection.InvalidSchemaResolution
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sdk.utils.Fixtures
import ai.senscience.nexus.delta.sourcing.model.Identity.User
import ai.senscience.nexus.delta.sourcing.model.{Label, ProjectRef, ResourceRef, Tags}
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.data.NonEmptyList
import cats.effect.IO
import io.circe.Json

import scala.collection.immutable.VectorMap

class SchemaImportsSuite extends NexusSuite with Fixtures {

  private val alice                = User("alice", Label.unsafe("wonderland"))
  implicit val aliceCaller: Caller = Caller(alice, Set(alice))

  private val neuroshapes       = "https://neuroshapes.org"
  private val parcellationlabel = iri"$neuroshapes/dash/parcellationlabel"
  private val json              = jsonContentOf("schemas/parcellationlabel.json")
  val projectRef                = ProjectRef.unsafe("org", "proj")

  val entitySource = jsonContentOf("schemas/entity.json")

  val entityExpandedSchema        = ExpandedJsonLd(jsonContentOf("schemas/entity-expanded.json")).accepted
  val identifierExpandedSchema    = ExpandedJsonLd(jsonContentOf("schemas/identifier-expanded.json")).accepted
  val licenseExpandedSchema       = ExpandedJsonLd(jsonContentOf("schemas/license-expanded.json")).accepted
  val propertyValueExpandedSchema = ExpandedJsonLd(jsonContentOf("schemas/property-value-expanded.json")).accepted

  val expandedSchemaMap = Map(
    iri"$neuroshapes/commons/entity" ->
      Schema(
        iri"$neuroshapes/commons/entity",
        projectRef,
        Tags.empty,
        entitySource,
        entityExpandedSchema.toCompacted(entitySource.topContextValueOrEmpty).accepted,
        NonEmptyList.of(
          entityExpandedSchema,
          identifierExpandedSchema,
          licenseExpandedSchema,
          propertyValueExpandedSchema
        )
      )
  )

  // format: off
  val resourceMap = VectorMap(
    iri"$neuroshapes/commons/vocabulary" -> jsonContentOf("schemas/vocabulary.json"),
    iri"$neuroshapes/wrong/vocabulary" -> jsonContentOf("schemas/vocabulary.json").replace("owl:Ontology", "owl:Other")
  ).map { case (iri, json) => iri -> ResourceGen.resource(iri, projectRef, json) }
  // format: on

  val errorReport = ResourceResolutionReport()

  val fetchSchema: Resolve[Schema]     = {
    case (ref, `projectRef`, _) => IO.pure(expandedSchemaMap.get(ref.iri).toRight(errorReport))
    case (_, _, _)              => IO.pure(Left(errorReport))
  }
  val fetchResource: Resolve[Resource] = {
    case (ref, `projectRef`, _) => IO.pure(resourceMap.get(ref.iri).toRight(errorReport))
    case (_, _, _)              => IO.pure(Left(errorReport))
  }

  private def toExpanded(json: Json) = ExpandedJsonLd(json)

  val imports = new SchemaImports(fetchSchema, fetchResource)

  test("Resolve all the imports") {
    for {
      expanded <- toExpanded(json)
      result   <- imports.resolve(parcellationlabel, projectRef, expanded)
    } yield {
      val expected = (resourceMap.take(1).values.map(_.expanded).toSet ++ Set(
        entityExpandedSchema,
        identifierExpandedSchema,
        licenseExpandedSchema,
        propertyValueExpandedSchema
      ) + expanded)
      assertEquals(result.toList.toSet, expected)
    }
  }

  test("Fail to resolve an import if it is not found") {
    val other        = iri"$neuroshapes/other"
    val other2       = iri"$neuroshapes/other2"
    val parcellation = json deepMerge json"""{"imports": ["$neuroshapes/commons/entity", "$other", "$other2"]}"""

    val expectedError = InvalidSchemaResolution(
      parcellationlabel,
      schemaImports = Map(ResourceRef(other) -> errorReport, ResourceRef(other2) -> errorReport),
      resourceImports = Map(ResourceRef(other) -> errorReport, ResourceRef(other2) -> errorReport),
      nonOntologyResources = Set.empty
    )

    toExpanded(parcellation).flatMap { expanded =>
      imports.resolve(parcellationlabel, projectRef, expanded).interceptEquals(expectedError)
    }
  }

  test("Fail to resolve an import if it is a resource without owl:Ontology type") {
    val wrong        = iri"$neuroshapes/wrong/vocabulary"
    val parcellation = json deepMerge json"""{"imports": ["$neuroshapes/commons/entity", "$wrong"]}"""

    val expectedError = InvalidSchemaResolution(
      parcellationlabel,
      schemaImports = Map(ResourceRef(wrong) -> errorReport),
      resourceImports = Map.empty,
      nonOntologyResources = Set(ResourceRef(wrong))
    )

    toExpanded(parcellation).flatMap { expanded =>
      imports.resolve(parcellationlabel, projectRef, expanded).interceptEquals(expectedError)
    }
  }
}
