package ai.senscience.nexus.delta.sourcing.stream.pipes

import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.rdf.jsonld.ExpandedJsonLd
import ai.senscience.nexus.delta.rdf.jsonld.decoder.JsonLdDecoder
import ai.senscience.nexus.delta.rdf.jsonld.decoder.semiauto.deriveDefaultJsonLdDecoder
import ai.senscience.nexus.delta.rdf.query.SparqlQuery.SparqlConstructQuery
import ai.senscience.nexus.delta.sourcing.state.GraphResource
import ai.senscience.nexus.delta.sourcing.stream.Elem.SuccessElem
import ai.senscience.nexus.delta.sourcing.stream.Operation.Pipe
import ai.senscience.nexus.delta.sourcing.stream.pipes.DataConstructQuery.DataConstructQueryConfig
import ai.senscience.nexus.delta.sourcing.stream.{Elem, PipeDef, PipeRef}
import cats.effect.IO
import io.circe.{Json, JsonObject}
import shapeless.Typeable

/**
  * Pipe implementation that transforms the resource graph with a provided query.
  */
class DataConstructQuery(config: DataConstructQueryConfig) extends Pipe {
  override type In  = GraphResource
  override type Out = GraphResource
  override def ref: PipeRef                     = DataConstructQuery.ref
  override def inType: Typeable[GraphResource]  = Typeable[GraphResource]
  override def outType: Typeable[GraphResource] = Typeable[GraphResource]

  override def apply(element: SuccessElem[GraphResource]): IO[Elem[GraphResource]] =
    element.value.graph.transform(config.query) match {
      case Left(err)       => IO.pure(element.failed(err))
      case Right(newGraph) => IO.pure(element.copy(value = element.value.copy(graph = newGraph)))
    }

}

/**
  * Pipe implementation that transforms the resource graph with a provided query.
  */
object DataConstructQuery extends PipeDef {
  override type PipeType = DataConstructQuery
  override type Config   = DataConstructQueryConfig
  override def configType: Typeable[Config]                                     = Typeable[DataConstructQueryConfig]
  override def configDecoder: JsonLdDecoder[Config]                             = JsonLdDecoder[DataConstructQueryConfig]
  override def ref: PipeRef                                                     = PipeRef.unsafe("dataConstructQuery")
  override def withConfig(config: DataConstructQueryConfig): DataConstructQuery = new DataConstructQuery(config)

  final case class DataConstructQueryConfig(query: SparqlConstructQuery) {
    def toJsonLd: ExpandedJsonLd = ExpandedJsonLd(
      Seq(
        ExpandedJsonLd.unsafe(
          nxv + ref.toString,
          JsonObject(
            (nxv + "query").toString -> Json.arr(Json.obj("@value" -> Json.fromString(query.value)))
          )
        )
      )
    )
  }
  object DataConstructQueryConfig                                        {
    implicit val dataConstructQueryConfigJsonLdDecoder: JsonLdDecoder[DataConstructQueryConfig] =
      deriveDefaultJsonLdDecoder
  }
}
