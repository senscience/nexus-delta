package ai.senscience.nexus.delta.sdk.schemas.model

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Triple.subject
import ai.senscience.nexus.delta.rdf.jsonld.ExpandedJsonLd
import ai.senscience.nexus.delta.rdf.syntax.*
import ai.senscience.nexus.delta.sdk.utils.Fixtures
import ai.senscience.nexus.delta.sourcing.model.{ProjectRef, Tags}
import ai.senscience.nexus.testkit.mu.NexusSuite
import cats.data.NonEmptyList
import munit.Location
import org.apache.jena.shacl.Shapes

import scala.jdk.CollectionConverters.*

class SchemaSuite extends NexusSuite with Fixtures {

  private val project = ProjectRef.unsafe("org", "proj")

  private def assertShapeSize(shapes: Shapes, id: Iri, expectedSize: Int)(using Location): Unit = {
    val shapesMap = shapes.getShapeMap.asScala
    assertEquals(
      shapesMap.get(subject(id)).map(_.getShapeGraph.size()),
      Some(expectedSize)
    )
  }

  test("Extract as a graph the content of the schema, removing the duplicates") {
    for {
      entitySource             <- loader.jsonContentOf("schemas/entity.json")
      entityExpanded           <- ExpandedJsonLd(jsonContentOf("schemas/entity-expanded.json"))
      entityExpandedGraphSize  <- entityExpanded.toGraph.map(_.getDefaultGraphSize)
      entityCompacted          <- entityExpanded.toCompacted(entitySource.topContextValueOrEmpty)
      licenseExpanded          <- ExpandedJsonLd(jsonContentOf("schemas/license-expanded.json"))
      licenseExpandedGraphSize <- licenseExpanded.toGraph.map(_.getDefaultGraphSize)
      id                        = iri"https://neuroshapes.org/commons/entity"
      expandeds                 = NonEmptyList.of(entityExpanded, licenseExpanded, entityExpanded)
      schema                    = Schema(id, project, Tags.empty, entitySource, entityCompacted, expandeds)
      shapes                   <- schema.shapes
    } yield {
      // There is also a propery shape with the schema:name
      assertEquals(shapes.numShapes(), 3)
      val expectedSize = entityExpandedGraphSize + licenseExpandedGraphSize
      assertShapeSize(shapes, iri"https://neuroshapes.org/commons/entity/shapes/EntityShape", expectedSize)
      assertShapeSize(shapes, iri"https://neuroshapes.org/commons/license/shapes/LicenseShape", expectedSize)
    }
  }

}
