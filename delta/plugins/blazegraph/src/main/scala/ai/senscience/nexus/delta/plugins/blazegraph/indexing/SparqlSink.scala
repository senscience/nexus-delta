package ai.senscience.nexus.delta.plugins.blazegraph.indexing

import ai.senscience.nexus.delta.kernel.error.HttpConnectivityError
import ai.senscience.nexus.delta.kernel.kamon.KamonMetricComponent
import ai.senscience.nexus.delta.kernel.syntax.kamonSyntax
import ai.senscience.nexus.delta.kernel.{Logger, RetryStrategy, RetryStrategyConfig}
import ai.senscience.nexus.delta.plugins.blazegraph.BlazegraphViews
import ai.senscience.nexus.delta.plugins.blazegraph.client.SparqlClientError.SparqlWriteError
import ai.senscience.nexus.delta.plugins.blazegraph.client.{SparqlClient, SparqlWriteQuery}
import ai.senscience.nexus.delta.plugins.blazegraph.indexing.SparqlSink.{logger, SparqlBulk}
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.RdfError.InvalidIri
import ai.senscience.nexus.delta.rdf.graph.NTriples
import ai.senscience.nexus.delta.rdf.syntax.*
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sourcing.stream.Operation.Sink
import ai.senscience.nexus.delta.sourcing.stream.config.BatchConfig
import ai.senscience.nexus.delta.sourcing.stream.{Elem, ElemChunk}
import cats.effect.IO
import org.http4s.Uri
import shapeless.Typeable

/**
  * Sink that pushed N-Triples into a given namespace in Sparql
  * @param client
  *   the SPARQL client
  * @param batchConfig
  *   the batch configuration for the sink
  * @param namespace
  *   the namespace
  */
final class SparqlSink(
    client: SparqlClient,
    override val batchConfig: BatchConfig,
    namespace: String,
    retryStrategy: RetryStrategy[Throwable]
)(implicit base: BaseUri)
    extends Sink {

  override type In = NTriples

  override def inType: Typeable[NTriples] = Typeable[NTriples]

  private val endpoint: Iri = base.endpoint.toIri

  implicit private val kamonComponent: KamonMetricComponent =
    KamonMetricComponent(BlazegraphViews.entityType.value)

  override def apply(elements: ElemChunk[NTriples]): IO[ElemChunk[Unit]] = {
    val bulk = elements.foldLeft(SparqlBulk.empty(endpoint)) {
      case (acc, Elem.SuccessElem(_, id, _, _, _, triples, _)) =>
        acc.replace(id, triples)
      case (acc, Elem.DroppedElem(_, id, _, _, _, _))          =>
        acc.drop(id)
      case (acc, _: Elem.FailedElem)                           =>
        acc
    }
    if (bulk.queries.nonEmpty)
      client
        .bulk(namespace, bulk.queries)
        .retry(retryStrategy)
        .redeemWith(
          err =>
            logger
              .error(err)(s"Indexing in sparql namespace $namespace failed")
              .as(elements.map { _.failed(err) }),
          _ => IO.pure(markInvalidIdsAsFailed(elements, bulk.invalidIds))
        ).span("sparqlSink")
    else
      IO.pure(markInvalidIdsAsFailed(elements, bulk.invalidIds))
  }

  private def markInvalidIdsAsFailed(elements: ElemChunk[NTriples], invalidIds: Set[Iri]) =
    elements.map { e =>
      if (invalidIds.contains(e.id))
        e.failed(InvalidIri)
      else
        e.void
    }

}

object SparqlSink {

  private val logger = Logger[SparqlSink]

  def apply(
      client: SparqlClient,
      retryStrategyConfig: RetryStrategyConfig,
      batchConfig: BatchConfig,
      namespace: String
  )(implicit base: BaseUri): SparqlSink = {
    val retryStrategy = RetryStrategy[Throwable](
      retryStrategyConfig,
      {
        case _: SparqlWriteError => true
        case e                   => HttpConnectivityError.test(e)
      },
      RetryStrategy.logError(logger, "sinking")(_, _)
    )
    new SparqlSink(client, batchConfig, namespace, retryStrategy)
  }

  final case class SparqlBulk(invalidIds: Set[Iri], queries: Vector[SparqlWriteQuery], endpoint: Iri) {

    private def parseUri(id: Iri) = Uri.fromString(id.resolvedAgainst(endpoint).toString)

    def replace(id: Iri, triples: NTriples): SparqlBulk =
      parseUri(id).fold(
        _ => copy(invalidIds = invalidIds + id),
        uri => copy(queries = queries :+ SparqlWriteQuery.replace(uri, triples))
      )

    def drop(id: Iri): SparqlBulk =
      parseUri(id).fold(
        _ => copy(invalidIds = invalidIds + id),
        uri => copy(queries = queries :+ SparqlWriteQuery.drop(uri))
      )

  }

  object SparqlBulk {
    def empty(endpoint: Iri): SparqlBulk = SparqlBulk(Set.empty, Vector.empty, endpoint)
  }
}
