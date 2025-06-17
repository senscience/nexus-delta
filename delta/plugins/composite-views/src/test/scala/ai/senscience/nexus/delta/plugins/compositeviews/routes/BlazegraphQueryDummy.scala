package ai.senscience.nexus.delta.plugins.compositeviews.routes

import ai.senscience.nexus.delta.plugins.blazegraph.client.SparqlQueryResponseType.Aux
import ai.senscience.nexus.delta.plugins.blazegraph.client.{SparqlClientError, SparqlQueryClient, SparqlQueryResponse}
import ai.senscience.nexus.delta.plugins.compositeviews.model.CompositeViewRejection.{ViewIsDeprecated, WrappedBlazegraphClientError}
import ai.senscience.nexus.delta.plugins.compositeviews.{BlazegraphQuery, CompositeViews}
import ai.senscience.nexus.delta.rdf.query.SparqlQuery
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.model.IdSegment
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import cats.effect.IO

class BlazegraphQueryDummy(client: SparqlQueryClient, views: CompositeViews) extends BlazegraphQuery {

  override def query[R <: SparqlQueryResponse](
      id: IdSegment,
      project: ProjectRef,
      query: SparqlQuery,
      responseType: Aux[R]
  )(implicit caller: Caller): IO[R] =
    for {
      view <- views.fetch(id, project)
      _    <- IO.raiseWhen(view.deprecated)(ViewIsDeprecated(view.id))
      res  <- client.query(Set("queryCommonNs"), query, responseType).adaptError { case e: SparqlClientError =>
                WrappedBlazegraphClientError(e)
              }
    } yield res

  override def query[R <: SparqlQueryResponse](
      id: IdSegment,
      projectionId: IdSegment,
      project: ProjectRef,
      query: SparqlQuery,
      responseType: Aux[R]
  )(implicit caller: Caller): IO[R] =
    for {
      view <- views.fetch(id, project)
      _    <- IO.raiseWhen(view.deprecated)(ViewIsDeprecated(view.id))
      res  <- client.query(Set("queryProjection"), query, responseType).adaptError { case e: SparqlClientError =>
                WrappedBlazegraphClientError(e)
              }
    } yield res

  override def queryProjections[R <: SparqlQueryResponse](
      id: IdSegment,
      project: ProjectRef,
      query: SparqlQuery,
      responseType: Aux[R]
  )(implicit caller: Caller): IO[R] =
    for {
      view <- views.fetch(id, project)
      _    <- IO.raiseWhen(view.deprecated)(ViewIsDeprecated(view.id))
      res  <- client.query(Set("queryProjections"), query, responseType).adaptError { case e: SparqlClientError =>
                WrappedBlazegraphClientError(e)
              }
    } yield res

}
