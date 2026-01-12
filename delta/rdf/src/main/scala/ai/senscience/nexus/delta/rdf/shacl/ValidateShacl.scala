package ai.senscience.nexus.delta.rdf.shacl

import ai.senscience.nexus.delta.rdf.graph.Graph
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import cats.effect.IO
import org.apache.jena.graph.Graph as JenaGraph
import org.apache.jena.shacl.engine.ValidationContext
import org.apache.jena.shacl.parser.Shape
import org.apache.jena.shacl.validation.VLib
import org.apache.jena.shacl.Shapes

import scala.jdk.CollectionConverters.*

final class ValidateShacl(shaclShapes: Shapes)(using RemoteContextResolution) {

  /**
    * Validates a given graph against the SHACL shapes spec.
    *
    * @param shapesGraph
    *   the shapes Graph to test against the SHACL shapes spec
    * @return
    *   an option of [[ValidationReport]] with the validation results
    */
  def apply(shapesGraph: Graph): IO[ValidationReport] =
    validate(shapesGraph, shaclShapes)

  /**
    * Validates a given data Graph against all shapes from a given shapes graph.
    *
    * @param graph
    *   the data Graph
    * @param shapes
    *   the shapes to validate against
    * @return
    *   an option of [[ValidationReport]] with the validation results
    */
  def apply(graph: Graph, shapes: Shapes): IO[ValidationReport] = {
    validate(graph, shapes)
  }

  private def validate(
      graph: Graph, shapes: Shapes
  ): IO[ValidationReport] = IO.delay {
    val data = graph.value.getDefaultGraph
    val vCtx = ValidationContext.create(shapes, data)
    val targetShapes = shapes.getTargetShapes.asScala
    val targetedNodes = targetShapes.foldLeft(0) { case (acc, shape) =>
      acc + validateShape(vCtx, data, shape)
    }
    (targetedNodes, vCtx)
  }.flatMap { case (targetedNodes, vCtx) => ValidationReport(targetedNodes, vCtx) }

  private def validateShape(vCtx: ValidationContext, data: JenaGraph, shape: Shape): Int =
    {
      val focusNodes = VLib.focusNodes(data, shape).asScala
      focusNodes.foreach { focusNode =>
        VLib.validateShape(vCtx, data, shape, focusNode)
      }
      focusNodes.size
    }
}

object ValidateShacl {

  def apply(rcr: RemoteContextResolution): IO[ValidateShacl] =
    ShaclFileLoader.readShaclShapes.map { shaclShaclShapes =>
      new ValidateShacl(shaclShaclShapes)(using rcr)
    }

}
