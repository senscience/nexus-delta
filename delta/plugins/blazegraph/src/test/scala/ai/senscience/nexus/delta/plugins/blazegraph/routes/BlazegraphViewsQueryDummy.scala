package ai.senscience.nexus.delta.plugins.blazegraph.routes

import ai.senscience.nexus.delta.plugins.blazegraph.client.SparqlQueryResponseType.Aux
import ai.senscience.nexus.delta.plugins.blazegraph.client.{SparqlQueryClient, SparqlQueryResponse}
import ai.senscience.nexus.delta.plugins.blazegraph.model.BlazegraphViewRejection.ViewIsDeprecated
import ai.senscience.nexus.delta.plugins.blazegraph.{BlazegraphViews, BlazegraphViewsQuery}
import ai.senscience.nexus.delta.rdf.query.SparqlQuery
import ai.senscience.nexus.delta.sdk.identities.model.Caller
import ai.senscience.nexus.delta.sdk.model.IdSegment
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import cats.effect.IO

private[routes] class BlazegraphViewsQueryDummy(
    client: SparqlQueryClient,
    views: BlazegraphViews
) extends BlazegraphViewsQuery {

  override def query[R <: SparqlQueryResponse](
      id: IdSegment,
      project: ProjectRef,
      query: SparqlQuery,
      responseType: Aux[R]
  )(implicit caller: Caller): IO[R] =
    for {
      view     <- views.fetch(id, project)
      _        <- IO.raiseWhen(view.deprecated)(ViewIsDeprecated(view.id))
      response <- client.query(Set(id.toString), query, responseType)
    } yield response

}
