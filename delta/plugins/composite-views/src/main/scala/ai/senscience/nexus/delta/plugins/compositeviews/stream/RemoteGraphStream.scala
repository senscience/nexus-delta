package ai.senscience.nexus.delta.plugins.compositeviews.stream

import ai.senscience.nexus.delta.plugins.compositeviews.client.DeltaClient
import ai.senscience.nexus.delta.plugins.compositeviews.indexing.MetadataPredicates
import ai.senscience.nexus.delta.plugins.compositeviews.model.CompositeViewSource.RemoteProjectSource
import ai.senscience.nexus.delta.plugins.compositeviews.stream.RemoteGraphStream.fromNQuads
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.RdfError.MissingPredicate
import ai.senscience.nexus.delta.rdf.Vocabulary.nxv
import ai.senscience.nexus.delta.rdf.graph.{Graph, NQuads}
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sourcing.model.{ProjectRef, ResourceRef}
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.state.GraphResource
import ai.senscience.nexus.delta.sourcing.stream.{Elem, ElemStream, RemainingElems, Source}
import cats.effect.IO
import cats.syntax.all.*
import io.circe.Json

final class RemoteGraphStream(deltaClient: DeltaClient, metadataPredicates: MetadataPredicates) {

  /**
    * Get a continuous stream of element as a [[Source]] for the main branch
    * @param remote
    *   the remote source
    */
  def main(remote: RemoteProjectSource): Source =
    Source(stream(remote, CompositeBranch.Run.Main))

  /**
    * Get the current elements as a [[Source]] for the rebuild branch
    * @param remote
    *   the remote source
    */
  def rebuild(remote: RemoteProjectSource): Source =
    Source(stream(remote, CompositeBranch.Run.Rebuild))

  private def stream(remote: RemoteProjectSource, run: CompositeBranch.Run): Offset => ElemStream[GraphResource] =
    deltaClient
      .elems(remote, run, _)
      .evalMap(populateElem(remote, _))

  private def populateElem(remote: RemoteProjectSource, elem: Elem[Unit]): IO[Elem[GraphResource]] =
    elem.evalMapFilter { _ =>
      deltaClient.resourceAsNQuads(remote, elem.id).flatMap {
        _.traverse { nquads => fromNQuads(elem, remote.project, nquads, metadataPredicates) }
      }
    }

  /**
    * Get information about the remaining elements
    * @param source
    *   the composite view source
    */
  def remaining(source: RemoteProjectSource, offset: Offset): IO[RemainingElems] =
    deltaClient.remaining(source, offset)

}

object RemoteGraphStream {

  private def findObject(metaGraph: Graph, subject: Iri, predicate: Iri) =
    Either.fromOption(metaGraph.find(subject, predicate), MissingPredicate(predicate))

  /**
    * Injects the elem value from the n-quads
    */
  def fromNQuads(
      elem: Elem[Unit],
      project: ProjectRef,
      nQuads: NQuads,
      metadataPredicates: MetadataPredicates
  ): IO[GraphResource] = IO.fromEither {
    for {
      graph                  <- Graph(nQuads)
      (metaGraph, valueGraph) = graph.partition { case (_, p, _) => metadataPredicates.values.contains(p) }
      types                   = graph.rootTypes
      schema                 <- findObject(metaGraph, elem.id, nxv.constrainedBy.iri)
                                  .map(triple => ResourceRef(iri"${triple.getURI}"))
      deprecated             <- findObject(metaGraph, elem.id, nxv.deprecated.iri)
                                  .map(_.getLiteralLexicalForm.toBoolean)
    } yield GraphResource(
      elem.tpe,
      project,
      elem.id,
      elem.rev,
      deprecated,
      schema,
      types,
      valueGraph,
      metaGraph,
      Json.obj()
    )
  }
}
