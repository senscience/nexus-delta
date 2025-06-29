package ai.senscience.nexus.delta.sourcing.stream.pipes

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.Vocabulary.*
import ai.senscience.nexus.delta.rdf.jsonld.decoder.JsonLdDecoder
import ai.senscience.nexus.delta.sourcing.stream.pipes.SelectPredicates.SelectPredicatesConfig
import ai.senscience.nexus.delta.sourcing.stream.{PipeDef, PipeRef}
import shapeless.Typeable

/**
  * Pipe implementation that transforms the resource graph keeping only the specific predicates defined in
  * [[DefaultLabelPredicates#defaultLabelPredicates]].
  */
object DefaultLabelPredicates extends PipeDef {
  override type PipeType = SelectPredicates
  override type Config   = Unit
  override def configType: Typeable[Config]         = Typeable[Unit]
  override def configDecoder: JsonLdDecoder[Config] = JsonLdDecoder[Unit]
  override def ref: PipeRef                         = PipeRef.unsafe("defaultLabelPredicates")

  override def withConfig(config: Unit): SelectPredicates = {
    val cfg = SelectPredicatesConfig(Some(Set(nxv + "File")), defaultLabelPredicates)
    new SelectPredicates(cfg)
  }

  val defaultLabelPredicates: Set[Iri] =
    Set(skos.prefLabel, rdf.tpe, rdfs.label, schema.name, schema.description, nxv.keywords.iri)
}
