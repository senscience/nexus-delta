package ai.senscience.nexus.delta.sdk.model.search

import ai.senscience.nexus.delta.kernel.search.Pagination
import ai.senscience.nexus.delta.kernel.search.Pagination.*
import ai.senscience.nexus.delta.rdf.Vocabulary.{contexts, nxv}
import ai.senscience.nexus.delta.rdf.instances.*
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.sdk.marshalling.HttpResponseFields
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.model.search.ResultEntry.UnscoredResultEntry
import cats.Functor
import cats.effect.IO
import cats.syntax.functor.*
import fs2.Stream
import io.circe.syntax.*
import io.circe.{Encoder, Json, JsonObject}
import org.http4s.Uri

/**
  * Defines the signature for a collection of search results with their metadata including pagination
  *
  * @tparam A
  *   the type of the result
  */
sealed trait SearchResults[A] extends Product with Serializable {
  def total: Long
  def token: Option[String]
  def results: Seq[ResultEntry[A]]
  def sources: Seq[A] = results.map(_.source)
}

object SearchResults {

  private val context = ContextValue(contexts.metadata, contexts.search)

  /**
    * A collection of search results with score including pagination.
    *
    * @param total
    *   the total number of results
    * @param maxScore
    *   the maximum score of the individual query results
    * @param results
    *   the collection of results
    * @param token
    *   the optional token used to generate the next link
    */
  final case class ScoredSearchResults[A](
      total: Long,
      maxScore: Float,
      results: Seq[ResultEntry[A]],
      token: Option[String] = None
  ) extends SearchResults[A]

  /**
    * A collection of query results including pagination.
    *
    * @param total
    *   the total number of results
    * @param results
    *   the collection of results
    * @param token
    *   the optional token used to generate the next link
    */
  final case class UnscoredSearchResults[A](total: Long, results: Seq[ResultEntry[A]], token: Option[String] = None)
      extends SearchResults[A]

  implicit final def scoredSearchResultsFunctor(implicit F: Functor[ResultEntry]): Functor[ScoredSearchResults] =
    new Functor[ScoredSearchResults] {
      override def map[A, B](fa: ScoredSearchResults[A])(f: A => B): ScoredSearchResults[B] =
        fa.copy(results = fa.results.map(qr => F.map(qr)(f)))
    }

  implicit final def unscoredSearchResultsFunctor(implicit F: Functor[ResultEntry]): Functor[UnscoredSearchResults] =
    new Functor[UnscoredSearchResults] {
      override def map[A, B](fa: UnscoredSearchResults[A])(f: A => B): UnscoredSearchResults[B] =
        fa.copy(results = fa.results.map(qr => F.map(qr)(f)))
    }

  implicit final def searchResultsFunctor(implicit F: Functor[ResultEntry]): Functor[SearchResults] =
    new Functor[SearchResults] {

      override def map[A, B](fa: SearchResults[A])(f: A => B): SearchResults[B] =
        fa match {
          case sqr: ScoredSearchResults[A]   => sqr.map(f)
          case uqr: UnscoredSearchResults[A] => uqr.map(f)
        }
    }

  /**
    * Constructs an [[ScoredSearchResults]]
    *
    * @param total
    *   the total number of results
    * @param maxScore
    *   the maximum score of the individual query results
    * @param results
    *   the collection of results
    */
  final def apply[A](total: Long, maxScore: Float, results: Seq[ResultEntry[A]]): SearchResults[A] =
    ScoredSearchResults[A](total, maxScore, results)

  /**
    * Constructs an [[UnscoredSearchResults]]
    *
    * @param total
    *   the total number of results
    * @param results
    *   the collection of results
    */
  final def apply[A](total: Long, results: Seq[A]): UnscoredSearchResults[A] =
    UnscoredSearchResults[A](total, results.map(UnscoredResultEntry(_)))

  final def apply[A](stream: Stream[IO, A]): IO[UnscoredSearchResults[A]] =
    stream.compile.toVector.map { values =>
      SearchResults(values.size.toLong, values)
    }

  final def apply[A](
      stream: Stream[IO, A],
      pagination: Pagination.FromPagination,
      ordering: Ordering[A]
  ): IO[UnscoredSearchResults[A]] =
    stream.compile.toVector
      .map { resources =>
        SearchResults(
          resources.size.toLong,
          resources.sorted(ordering).slice(pagination.from, pagination.from + pagination.size)
        )
      }

  /**
    * Builds an [[JsonLdEncoder]] of [[SearchResults]] of ''A'' where the next link is computed using the passed
    * ''pagination'' and ''searchUri''
    */
  def searchResultsJsonLdEncoder[A: Encoder.AsObject](
      additionalContext: ContextValue,
      pagination: Pagination,
      searchUri: Uri
  )(implicit baseUri: BaseUri): JsonLdEncoder[SearchResults[A]] = {
    val nextLink: SearchResults[A] => Option[Uri] = results =>
      pagination -> results.token match {
        case (_: SearchAfterPagination, None)                    => None
        case (p: Pagination, _) if p.size > results.results.size => None
        case _ if results.results.size >= results.total          => None
        case (_, Some(token))                                    => Some(next(searchUri, token))
        case (p: FromPagination, _)                              => Some(next(searchUri, p))
      }
    searchResultsJsonLdEncoder(additionalContext, nextLink)

  }

  /**
    * Builds an [[JsonLdEncoder]] of [[SearchResults]] of ''A'' where there is no next link
    */
  def searchResultsJsonLdEncoder[A: Encoder.AsObject](
      additionalContext: ContextValue
  ): JsonLdEncoder[SearchResults[A]] =
    searchResultsJsonLdEncoder(additionalContext, _ => None)

  private def searchResultsJsonLdEncoder[A: Encoder.AsObject](
      additionalContext: ContextValue,
      next: SearchResults[A] => Option[Uri]
  ): JsonLdEncoder[SearchResults[A]] = {
    implicit val encoder: Encoder.AsObject[SearchResults[A]] = searchResultsEncoder(next)
    JsonLdEncoder.computeFromCirce(context.merge(additionalContext))
  }

  def searchResultsEncoder[A: Encoder.AsObject](
      next: SearchResults[A] => Option[Uri]
  ): Encoder.AsObject[SearchResults[A]] =
    Encoder.AsObject.instance { r =>
      val common = JsonObject(
        nxv.total.prefix   -> Json.fromLong(r.total),
        nxv.results.prefix -> Json.fromValues(r.results.map(_.asJson)),
        nxv.next.prefix    -> next(r).asJson
      )
      r match {
        case ScoredSearchResults(_, maxScore, _, _) => common.add(nxv.maxScore.prefix, maxScore.asJson)
        case _                                      => common
      }
    }

  private def next(
      current: Uri,
      nextToken: String
  )(implicit baseUri: BaseUri): Uri =
    toPublic(current)
      .removeQueryParam(from)
      .withQueryParam(after, nextToken)

  private def next(
      current: Uri,
      pagination: FromPagination
  )(implicit baseUri: BaseUri): Uri = {
    val nextFrom = pagination.from + pagination.size
    toPublic(current)
      .withQueryParam(from, nextFrom.toString)
      .withQueryParam(size, pagination.size.toString)
  }

  private def toPublic(uri: Uri)(implicit baseUri: BaseUri): Uri =
    uri.copy(scheme = baseUri.scheme, authority = baseUri.authority)

  def empty[A]: SearchResults[A] = UnscoredSearchResults(0L, Seq.empty)

  implicit def searchResultsHttpResponseFields[A]: HttpResponseFields[SearchResults[A]] = HttpResponseFields.defaultOk
}
