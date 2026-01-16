package ai.senscience.nexus.delta.elasticsearch.indexing

import ai.senscience.nexus.delta.elasticsearch.ElasticSearchViews
import ai.senscience.nexus.delta.elasticsearch.client.IndexLabel
import ai.senscience.nexus.delta.elasticsearch.model.{ElasticSearchViewState, ElasticsearchIndexDef}
import ai.senscience.nexus.delta.elasticsearch.views.DefaultIndexDef
import ai.senscience.nexus.delta.elasticsearch.views.DefaultIndexDef.fallbackUnless
import ai.senscience.nexus.delta.kernel.Logger
import ai.senscience.nexus.delta.rdf.jsonld.api.{JsonLdApi, TitaniumJsonLdApi}
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue.ContextObject
import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.sdk.stream.GraphResourceStream
import ai.senscience.nexus.delta.sdk.views.{IndexingRev, ViewRef}
import ai.senscience.nexus.delta.sourcing.offset.Offset
import ai.senscience.nexus.delta.sourcing.query.SelectFilter
import ai.senscience.nexus.delta.sourcing.state.GraphResource
import ai.senscience.nexus.delta.sourcing.stream.*
import ai.senscience.nexus.delta.sourcing.stream.Operation.Sink
import cats.data.NonEmptyChain
import cats.effect.IO
import cats.syntax.all.*

/**
  * Definition of a view to build a projection
  */
sealed trait IndexingViewDef extends Product with Serializable {

  def ref: ViewRef

  override def toString: String = s"${ref.project}/${ref.viewId}"

}

object IndexingViewDef {

  private val logger = Logger[IndexingViewDef]

  /**
    * Active view eligible to be run as a projection by the supervisor
    */
  final case class ActiveViewDef(
      ref: ViewRef,
      projection: String,
      pipeChain: Option[PipeChain],
      selectFilter: SelectFilter,
      index: IndexLabel,
      indexDef: ElasticsearchIndexDef,
      context: Option[ContextObject],
      indexingRev: IndexingRev,
      rev: Int
  ) extends IndexingViewDef {

    def projectionMetadata: ProjectionMetadata =
      ProjectionMetadata(
        ElasticSearchViews.entityType.value,
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
      state: ElasticSearchViewState,
      defaultDef: DefaultIndexDef,
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
          ElasticSearchViews.projectionName(state),
          indexing.pipeChain,
          indexing.selectFilter,
          ElasticSearchViews.index(state.uuid, state.indexingRev, prefix),
          defaultDef.fallbackUnless(indexing.mapping, indexing.settings),
          indexing.context,
          state.indexingRev,
          state.rev
        )

    }

  def compile(
      v: ActiveViewDef,
      pipeChainCompiler: PipeChainCompiler,
      elems: ElemStream[GraphResource],
      sink: Sink
  )(using RemoteContextResolution): IO[CompiledProjection] =
    compile(v, pipeChainCompiler, _ => elems, sink)

  def compile(
      v: ActiveViewDef,
      pipeChainCompiler: PipeChainCompiler,
      graphStream: GraphResourceStream,
      sink: Sink
  )(using RemoteContextResolution): IO[CompiledProjection] =
    compile(v, pipeChainCompiler, graphStream.continuous(v.ref.project, v.selectFilter, _), sink)

  private def compile(
      v: ActiveViewDef,
      pipeChainCompiler: PipeChainCompiler,
      stream: Offset => ElemStream[GraphResource],
      sink: Sink
  )(using RemoteContextResolution): IO[CompiledProjection] = {

    val mergedContext        = v.context.fold(defaultIndexingContext) { defaultIndexingContext.merge(_) }
    given JsonLdApi          = TitaniumJsonLdApi.lenient
    val postPipes: Operation = new GraphResourceToDocument(mergedContext, false)

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
