package ai.senscience.nexus.delta.rdf.shacl

import ai.senscience.nexus.delta.rdf.IriOrBNode.BNode
import ai.senscience.nexus.delta.rdf.Triple.predicate
import ai.senscience.nexus.delta.rdf.Vocabulary.{contexts, nxsh, sh}
import ai.senscience.nexus.delta.rdf.graph.Graph
import ai.senscience.nexus.delta.rdf.jsonld.api.{JsonLdApi, TitaniumJsonLdApi}
import ai.senscience.nexus.delta.rdf.jsonld.context.{ContextValue, RemoteContextResolution}
import ai.senscience.nexus.delta.rdf.syntax.iriStringContextSyntax
import cats.effect.IO
import io.circe.{Encoder, Json}
import org.apache.jena.query.DatasetFactory
import org.apache.jena.rdf.model.ResourceFactory
import org.apache.jena.shacl.engine.ValidationContext

/**
  * Data type that represents the outcome of validating data against a shacl schema.
  */
final case class ValidationReport private (conforms: Boolean, targetedNodes: Int, json: Json) {

  def withTargetedNodes: Boolean = targetedNodes > 0

  def conformsWithTargetedNodes: Boolean = conforms && withTargetedNodes
}

object ValidationReport {

  private val shaclCtx: ContextValue = ContextValue(contexts.shacl)

  private val targetNodeProperty = ResourceFactory.createProperty(nxsh.targetedNodes.toString)

  final def apply(targetedNodes: Int, vCtx: ValidationContext)(using RemoteContextResolution): IO[ValidationReport] = {
    given JsonLdApi = TitaniumJsonLdApi.lenient
    for {
      report        <- IO.delay { vCtx.generateReport() }
      reportResource = report.getResource.addLiteral(targetNodeProperty, targetedNodes)
      tmpGraph      <- IO.delay(Graph.unsafe(DatasetFactory.create(reportResource.getModel).asDatasetGraph()))
      rootNode      <-
        IO.fromEither(
          tmpGraph
            .find { case (_, p, _) => p == predicate(sh.conforms) }
            .map { case (s, _, _) => if s.isURI then iri"${s.getURI}" else BNode.unsafe(s.getBlankNodeLabel) }
            .toRight(new IllegalStateException("Unable to find predicate sh:conforms in the validation report graph"))
        )
      graph          = tmpGraph.replaceRootNode(rootNode)
      compacted     <- graph.toCompactedJsonLd(shaclCtx)
    } yield ValidationReport(report.conforms(), targetedNodes, compacted.json)
  }

  def unsafe(conforms: Boolean, targetedNodes: Int, json: Json): ValidationReport =
    ValidationReport(conforms, targetedNodes, json)

  given Encoder[ValidationReport] = Encoder.instance(_.json)
}
