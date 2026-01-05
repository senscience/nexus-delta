package ai.senscience.nexus.delta.elasticsearch.indexing

import ai.senscience.nexus.delta.elasticsearch.configured.ConfiguredIndexDocument
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.model.source.OriginalSource
import ai.senscience.nexus.delta.sourcing.stream.Elem.SuccessElem
import ai.senscience.nexus.delta.sourcing.stream.Operation.Pipe
import ai.senscience.nexus.delta.sourcing.stream.{Elem, PipeRef}
import cats.effect.IO
import io.circe.syntax.EncoderOps
import shapeless3.typeable.Typeable

/**
  * Pipe that transforms a [[MainDocumentResource]] into a configured index document
  */
final class AnnotatedSourceToConfiguredDocument(configuredTypes: Set[Iri])(using BaseUri) extends Pipe {
  override type In  = OriginalSource.Annotated
  override type Out = ConfiguredIndexDocument
  override def ref: PipeRef           = PipeRef.unsafe("annotated-source-to-document")
  override def inType: Typeable[In]   = Typeable[OriginalSource.Annotated]
  override def outType: Typeable[Out] = Typeable[ConfiguredIndexDocument]

  override def apply(element: SuccessElem[OriginalSource.Annotated]): IO[Elem[ConfiguredIndexDocument]] =
    element.evalMapFilter { annotated =>
      IO.pure(
        Option.when(annotated.resourceF.types.exists(configuredTypes.contains))(
          ConfiguredIndexDocument(annotated.resourceF.types, annotated.asJson)
        )
      )
    }
}
