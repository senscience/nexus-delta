package ai.senscience.nexus.delta.sourcing.stream.pipes

import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.rdf.implicits.{given, *}
import ai.senscience.nexus.delta.rdf.jsonld.context.JsonLdContext.keywords
import ai.senscience.nexus.delta.rdf.jsonld.decoder.JsonLdDecoder
import ai.senscience.nexus.delta.sourcing.state.GraphResource
import ai.senscience.nexus.delta.sourcing.stream.Elem.SuccessElem
import ai.senscience.nexus.delta.sourcing.stream.Operation.Pipe
import ai.senscience.nexus.delta.sourcing.stream.{Elem, PipeDef, PipeRef}
import cats.effect.IO
import io.circe.Json
import shapeless3.typeable.Typeable

/**
  * Pipe implementation that embeds the resource source into the metadata graph.
  */
class SourceAsText extends Pipe {

  private val empty = Json.obj()

  override type In  = GraphResource
  override type Out = GraphResource
  override def ref: PipeRef                     = SourceAsText.ref
  override def inType: Typeable[GraphResource]  = Typeable[GraphResource]
  override def outType: Typeable[GraphResource] = Typeable[GraphResource]

  override def apply(element: SuccessElem[GraphResource]): IO[Elem[GraphResource]] = {
    val graph = element.value.metadataGraph
      .add(nxv.originalSource.iri, element.value.source.removeAllKeys(keywords.context).noSpaces)
    IO.pure(element.map(state => state.copy(metadataGraph = graph, source = empty)))
  }

}

/**
  * Pipe implementation that embeds the resource source into the metadata graph.
  */
object SourceAsText extends PipeDef {
  override type PipeType = SourceAsText
  override type Config   = Unit
  override def configType: Typeable[Config]           = Typeable[Unit]
  override def configDecoder: JsonLdDecoder[Config]   = JsonLdDecoder[Unit]
  override def ref: PipeRef                           = PipeRef.unsafe("sourceAsText")
  override def withConfig(config: Unit): SourceAsText = new SourceAsText
}
