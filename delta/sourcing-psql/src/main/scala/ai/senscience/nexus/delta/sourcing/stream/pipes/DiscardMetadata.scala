package ai.senscience.nexus.delta.sourcing.stream.pipes

import ai.senscience.nexus.delta.rdf.graph.Graph
import ai.senscience.nexus.delta.rdf.jsonld.ExpandedJsonLd
import ai.senscience.nexus.delta.rdf.jsonld.decoder.JsonLdDecoder
import ai.senscience.nexus.delta.sourcing.state.GraphResource
import ai.senscience.nexus.delta.sourcing.stream.Elem.SuccessElem
import ai.senscience.nexus.delta.sourcing.stream.Operation.Pipe
import ai.senscience.nexus.delta.sourcing.stream.{Elem, PipeDef, PipeRef}
import cats.effect.IO
import shapeless.Typeable

/**
  * Pipe implementation that drops the contents of the state metadata graph.
  */
class DiscardMetadata extends Pipe {
  override type In  = GraphResource
  override type Out = GraphResource
  override def ref: PipeRef                     = DiscardMetadata.ref
  override def inType: Typeable[GraphResource]  = Typeable[GraphResource]
  override def outType: Typeable[GraphResource] = Typeable[GraphResource]

  override def apply(element: SuccessElem[GraphResource]): IO[Elem[GraphResource]] =
    IO.pure(element.map(state => state.copy(metadataGraph = Graph.empty(element.value.id))))

}

/**
  * Pipe implementation that drops the contents of the state metadata graph.
  */
object DiscardMetadata extends PipeDef {
  override type PipeType = DiscardMetadata
  override type Config   = Unit
  override def configType: Typeable[Config]              = Typeable[Unit]
  override def configDecoder: JsonLdDecoder[Config]      = JsonLdDecoder[Unit]
  override def ref: PipeRef                              = PipeRef.unsafe("discardMetadata")
  override def withConfig(config: Unit): DiscardMetadata = new DiscardMetadata

  /**
    * Returns the pipe ref and its empty config
    */
  def apply(): (PipeRef, ExpandedJsonLd) = ref -> ExpandedJsonLd.empty
}
