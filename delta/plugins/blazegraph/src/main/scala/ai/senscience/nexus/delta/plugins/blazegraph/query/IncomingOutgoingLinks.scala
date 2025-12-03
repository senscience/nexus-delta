package ai.senscience.nexus.delta.plugins.blazegraph.query

import ai.senscience.nexus.delta.kernel.search.Pagination.FromPagination
import ai.senscience.nexus.delta.kernel.utils.ClasspathResourceLoader
import ai.senscience.nexus.delta.plugins.blazegraph.BlazegraphViews
import ai.senscience.nexus.delta.plugins.blazegraph.client.SparqlQueryResponseType.SparqlResultsJson
import ai.senscience.nexus.delta.plugins.blazegraph.client.{SparqlQueryClient, SparqlResults}
import ai.senscience.nexus.delta.plugins.blazegraph.model.BlazegraphViewRejection.InvalidResourceId
import ai.senscience.nexus.delta.plugins.blazegraph.model.SparqlLink.{SparqlExternalLink, SparqlResourceLink}
import ai.senscience.nexus.delta.plugins.blazegraph.model.{defaultViewId, BlazegraphViewRejection, SparqlLink}
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.rdf.query.SparqlQuery
import ai.senscience.nexus.delta.sdk.jsonld.ExpandIri
import ai.senscience.nexus.delta.sdk.model.IdSegment.IriSegment
import ai.senscience.nexus.delta.sdk.model.search.ResultEntry.UnscoredResultEntry
import ai.senscience.nexus.delta.sdk.model.search.SearchResults
import ai.senscience.nexus.delta.sdk.model.search.SearchResults.UnscoredSearchResults
import ai.senscience.nexus.delta.sdk.model.{BaseUri, IdSegment}
import ai.senscience.nexus.delta.sdk.projects.FetchContext
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import cats.effect.IO
import org.typelevel.otel4s.{Attribute, AttributeKey}
import org.typelevel.otel4s.trace.Tracer

import java.util.regex.Pattern.quote

trait IncomingOutgoingLinks {

  /**
    * List incoming links for a given resource.
    *
    * @param resourceId
    *   the resource identifier
    * @param project
    *   the project of the resource
    * @param pagination
    *   the pagination config
    */
  def incoming(resourceId: IdSegment, project: ProjectRef, pagination: FromPagination): IO[SearchResults[SparqlLink]]

  /**
    * List outgoing links for a given resource.
    *
    * @param resourceId
    *   the resource identifier
    * @param project
    *   the project of the resource
    * @param pagination
    *   the pagination config
    * @param includeExternalLinks
    *   whether to include links to resources not managed by Delta
    */
  def outgoing(
      resourceId: IdSegment,
      project: ProjectRef,
      pagination: FromPagination,
      includeExternalLinks: Boolean
  ): IO[SearchResults[SparqlLink]]

}

object IncomingOutgoingLinks {

  private val loader = ClasspathResourceLoader.withContext(getClass)

  final case class Queries(incoming: String, outgoingWithExternal: String, outgoingScoped: String)

  object Queries {
    def load: IO[Queries] = for {
      incoming             <- loader.contentOf("blazegraph/incoming.txt")
      outgoingWithExternal <- loader.contentOf("blazegraph/outgoing_include_external.txt")
      outgoingScoped       <- loader.contentOf("blazegraph/outgoing_scoped.txt")
    } yield Queries(incoming, outgoingWithExternal, outgoingScoped)
  }

  def apply(fetchContext: FetchContext, views: BlazegraphViews, client: SparqlQueryClient, queries: Queries)(using
      BaseUri,
      Tracer[IO]
  ): IncomingOutgoingLinks = {
    def fetchNamespace: ProjectRef => IO[String] =
      views.fetchIndexingView(IriSegment(defaultViewId), _).map(_.namespace)
    apply(fetchContext, fetchNamespace, client, queries)
  }

  def apply(
      fetchContext: FetchContext,
      fetchDefaultNamespace: ProjectRef => IO[String],
      client: SparqlQueryClient,
      queries: Queries
  )(using BaseUri, Tracer[IO]): IncomingOutgoingLinks = new IncomingOutgoingLinks {

    private val expandIri: ExpandIri[BlazegraphViewRejection] = new ExpandIri(InvalidResourceId.apply)

    private val includeExternalLinksKey = AttributeKey[Boolean]("include-external-links")

    private def includeExternal(includeExternalLinks: Boolean) =
      Attribute(includeExternalLinksKey, includeExternalLinks)

    override def incoming(
        resourceId: IdSegment,
        project: ProjectRef,
        pagination: FromPagination
    ): IO[SearchResults[SparqlLink]] = {
      for {
        iri       <- expandResourceIri(resourceId, project)
        namespace <- fetchDefaultNamespace(project)
        q          = SparqlQuery(replace(queries.incoming, iri, pagination))
        bindings  <- client.query(Set(namespace), q, SparqlResultsJson)
        links      = toSparqlLinks(bindings.value)
      } yield links
    }.surround("incoming")

    override def outgoing(
        resourceId: IdSegment,
        project: ProjectRef,
        pagination: FromPagination,
        includeExternalLinks: Boolean
    ): IO[SearchResults[SparqlLink]] = {
      for {
        iri          <- expandResourceIri(resourceId, project)
        namespace    <- fetchDefaultNamespace(project)
        queryTemplate = if includeExternalLinks then queries.outgoingWithExternal else queries.outgoingScoped
        q             = SparqlQuery(replace(queryTemplate, iri, pagination))
        bindings     <- client.query(Set(namespace), q, SparqlResultsJson)
        links         = toSparqlLinks(bindings.value)
      } yield links
    }.surround("outgoing", includeExternal(includeExternalLinks))

    private def expandResourceIri(resourceId: IdSegment, project: ProjectRef) =
      fetchContext.onRead(project).flatMap { pc => expandIri(resourceId, pc) }

    private def replace(query: String, id: Iri, pagination: FromPagination): String =
      query
        .replaceAll(quote("{id}"), id.toString)
        .replaceAll(quote("{offset}"), pagination.from.toString)
        .replaceAll(quote("{size}"), pagination.size.toString)

    private def toSparqlLinks(sparqlResults: SparqlResults): SearchResults[SparqlLink] = {
      val (count, results) =
        sparqlResults.results.bindings
          .foldLeft((0L, List.empty[SparqlLink])) { case ((total, acc), bindings) =>
            val newTotal = bindings.get("total").flatMap(v => v.value.toLongOption).getOrElse(total)
            val res      = (SparqlResourceLink(bindings) orElse SparqlExternalLink(bindings))
              .map(_ :: acc)
              .getOrElse(acc)
            (newTotal, res)
          }
      UnscoredSearchResults(count, results.map(UnscoredResultEntry(_)))
    }
  }

}
