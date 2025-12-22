package ai.senscience.nexus.delta.elasticsearch.indexing

import ai.senscience.nexus.delta.sdk.indexing.MainDocument
import ai.senscience.nexus.delta.sourcing.stream.Elem.SuccessElem
import ai.senscience.nexus.delta.sourcing.stream.Operation.Pipe
import ai.senscience.nexus.delta.sourcing.stream.{Elem, PipeRef}
import cats.effect.IO
import io.circe.Json
import shapeless3.typeable.Typeable

/**
  * Pipe that transforms a [[MainDocumentResource]] into a Json document
  */
object MainDocumentToJson extends Pipe {
  override type In  = MainDocument
  override type Out = Json
  override def ref: PipeRef           = PipeRef.unsafe("main-document-to-json")
  override def inType: Typeable[In]   = Typeable[MainDocument]
  override def outType: Typeable[Out] = Typeable[Json]

  override def apply(element: SuccessElem[MainDocument]): IO[Elem[Json]] =
    IO.pure(element.map { _.payload })
}
