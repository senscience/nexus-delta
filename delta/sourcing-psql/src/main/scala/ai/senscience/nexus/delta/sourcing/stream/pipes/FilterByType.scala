package ai.senscience.nexus.delta.sourcing.stream.pipes

import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.rdf.jsonld.ExpandedJsonLd
import ai.senscience.nexus.delta.rdf.jsonld.decoder.JsonLdDecoder
import ai.senscience.nexus.delta.rdf.jsonld.decoder.semiauto.deriveDefaultJsonLdDecoder
import ai.senscience.nexus.delta.sourcing.model.IriFilter
import ai.senscience.nexus.delta.sourcing.state.GraphResource
import ai.senscience.nexus.delta.sourcing.stream.Elem.SuccessElem
import ai.senscience.nexus.delta.sourcing.stream.Operation.Pipe
import ai.senscience.nexus.delta.sourcing.stream.pipes.FilterByType.FilterByTypeConfig
import ai.senscience.nexus.delta.sourcing.stream.{Elem, PipeDef, PipeRef}
import cats.effect.IO
import io.circe.syntax.EncoderOps
import io.circe.{Json, JsonObject}
import shapeless3.typeable.Typeable

/**
  * Pipe implementation that filters resources based on their type.
  */
class FilterByType(config: FilterByTypeConfig) extends Pipe {
  override type In  = GraphResource
  override type Out = GraphResource
  override def ref: PipeRef                     = FilterByType.ref
  override def inType: Typeable[GraphResource]  = Typeable[GraphResource]
  override def outType: Typeable[GraphResource] = Typeable[GraphResource]

  // TODO duplicated logic for schemas and types
  override def apply(element: SuccessElem[GraphResource]): IO[Elem[GraphResource]] = config.types match {
    case IriFilter.None                                                         => IO.pure(element)
    case IriFilter.Include(types) if types.exists(element.value.types.contains) => IO.pure(element)
    case IriFilter.Include(_)                                                   => IO.pure(element.dropped)
  }
}

/**
  * Pipe implementation that filters resources based on their type.
  */
object FilterByType extends PipeDef {
  override type PipeType = FilterByType
  override type Config   = FilterByTypeConfig
  override def configType: Typeable[Config]                         = Typeable[FilterByTypeConfig]
  override def configDecoder: JsonLdDecoder[Config]                 = JsonLdDecoder[FilterByTypeConfig]
  override def ref: PipeRef                                         = PipeRef.unsafe("filterByType")
  override def withConfig(config: FilterByTypeConfig): FilterByType = new FilterByType(config)

  final case class FilterByTypeConfig(types: IriFilter) {
    def toJsonLd: ExpandedJsonLd = ExpandedJsonLd(
      Seq(
        ExpandedJsonLd.unsafe(
          nxv + ref.toString,
          JsonObject(
            (nxv + "types").toString -> Json.arr(
              types.asRestrictedTo
                .map(_.iris.toList)
                .getOrElse(List.empty)
                .map(iri => Json.obj("@id" -> iri.asJson))*
            )
          )
        )
      )
    )
  }
  object FilterByTypeConfig                             {
    given JsonLdDecoder[FilterByTypeConfig] = deriveDefaultJsonLdDecoder
  }

  /**
    * Returns the pipe ref and config from the provided types
    */
  def apply(types: IriFilter): (PipeRef, ExpandedJsonLd) = ref -> FilterByTypeConfig(types).toJsonLd
}
