package ai.senscience.nexus.delta.plugins.blazegraph.indexing

import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.plugins.blazegraph.BlazegraphViews
import ai.senscience.nexus.delta.plugins.blazegraph.model.BlazegraphViewState
import ai.senscience.nexus.delta.sdk.stream.GraphResourceStream
import ai.senscience.nexus.delta.sdk.views.ViewRef
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.query.SelectFilter
import ai.senscience.nexus.delta.sourcing.state.GraphResource
import ai.senscience.nexus.delta.sourcing.stream.*
import ai.senscience.nexus.delta.sourcing.stream.Operation.Sink
import cats.data.NonEmptyChain
import cats.effect.IO
import cats.syntax.all.*

/**
  * Definition of a Blazegraph view to build a projection
  */
sealed trait IndexingViewDef extends Product with Serializable {

  def ref: ViewRef

}

object IndexingViewDef {

  private val logger = Logger[IndexingViewDef]

  /**
    * Active view eligible to be run as a projection by the supervisor
    */
  final case class ActiveViewDef(
      ref: ViewRef,
      projection: String,
      selectFilter: SelectFilter,
      pipeChain: Option[PipeChain],
      namespace: String,
      indexingRev: Int,
      rev: Int
  ) extends IndexingViewDef {
    def projectionMetadata: ProjectionMetadata =
      ProjectionMetadata(
        BlazegraphViews.entityType.value,
        projection,
        Some(ref.project),
        Some(ref.viewId)
      )
  }

  /**
    * Deprecated view to be cleaned up and removed from the supervisor
    */
  final case class DeprecatedViewDef(ref: ViewRef) extends IndexingViewDef

  def apply(
      state: BlazegraphViewState,
      prefix: String
  ): Option[IndexingViewDef] =
    state.value.asIndexingValue.map { indexing =>
      if state.deprecated then
        DeprecatedViewDef(
          ViewRef(state.project, state.id)
        )
      else
        ActiveViewDef(
          ViewRef(state.project, state.id),
          BlazegraphViews.projectionName(state),
          indexing.selectFilter,
          indexing.pipeChain,
          BlazegraphViews.namespace(state.uuid, state.indexingRev, prefix),
          state.indexingRev,
          state.rev
        )
    }

  def compile(
      v: ActiveViewDef,
      pipeChainCompiler: PipeChainCompiler,
      elems: ElemStream[GraphResource],
      sink: Sink
  ): IO[CompiledProjection] =
    compile(v, pipeChainCompiler, _ => elems, sink)

  def compile(
      v: ActiveViewDef,
      pipeChainCompiler: PipeChainCompiler,
      graphStream: GraphResourceStream,
      sink: Sink
  ): IO[CompiledProjection] =
    compile(
      v,
      pipeChainCompiler,
      graphStream.continuous(v.ref.project, v.selectFilter, _),
      sink
    )

  private def compile(
      v: ActiveViewDef,
      pipeChainCompiler: PipeChainCompiler,
      stream: Offset => ElemStream[GraphResource],
      sink: Sink
  ): IO[CompiledProjection] = {

    val postPipes: Operation = GraphResourceToNTriples

    val compiled = for {
      pipes      <- v.pipeChain.traverse(pipeChainCompiler(_))
      chain       = pipes.fold(NonEmptyChain.one(postPipes))(NonEmptyChain(_, postPipes))
      projection <- CompiledProjection.compile(
                      v.projectionMetadata,
                      ExecutionStrategy.PersistentSingleNode,
                      Source(stream),
                      chain,
                      sink
                    )
    } yield projection

    IO.fromEither(compiled).onError { case e =>
      logger.error(e)(s"View '${v.ref}' could not be compiled.")
    }
  }
}
