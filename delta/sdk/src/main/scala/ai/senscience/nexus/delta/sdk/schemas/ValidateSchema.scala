package ai.senscience.nexus.delta.sdk.schemas

import ai.senscience.nexus.delta.kernel.syntax.*
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.RdfError
import ai.senscience.nexus.delta.rdf.graph.Graph
import ai.senscience.nexus.delta.rdf.jsonld.ExpandedJsonLd
import ai.senscience.nexus.delta.rdf.jsonld.api.{JsonLdApi, TitaniumJsonLdApi}
import ai.senscience.nexus.delta.rdf.shacl.{ValidateShacl, ValidationReport}
import ai.senscience.nexus.delta.sdk.jsonld.JsonLdRejection.InvalidJsonLdFormat
import ai.senscience.nexus.delta.sdk.schemas.Schemas.kamonComponent
import cats.data.NonEmptyList
import cats.effect.IO
import cats.syntax.all.*

trait ValidateSchema {

  def apply(id: Iri, expanded: NonEmptyList[ExpandedJsonLd]): IO[ValidationReport]

}

object ValidateSchema {

  def apply(validateShacl: ValidateShacl): ValidateSchema =
    new ValidateSchema {
      implicit val api: JsonLdApi = TitaniumJsonLdApi.strict

      override def apply(id: Iri, expanded: NonEmptyList[ExpandedJsonLd]): IO[ValidationReport] = {
        for {
          graph  <- toGraph(id, expanded)
          report <- validateShacl(graph, reportDetails = true)
        } yield report
      }.span("validateShacl")

      private def toGraph(id: Iri, expanded: NonEmptyList[ExpandedJsonLd]) =
        toFoldableOps(expanded)
          .foldM(Graph.empty)((acc, expandedEntry) => expandedEntry.toGraph.map(acc ++ (_: Graph)))
          .adaptError { case err: RdfError => InvalidJsonLdFormat(Some(id), err) }
    }

}
