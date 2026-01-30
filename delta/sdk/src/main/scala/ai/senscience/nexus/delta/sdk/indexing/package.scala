package ai.senscience.nexus.delta.sdk

import ai.senscience.nexus.delta.kernel.search.Pagination
import ai.senscience.nexus.delta.rdf.Vocabulary.contexts
import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue
import ai.senscience.nexus.delta.rdf.jsonld.context.JsonLdContext.keywords
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.model.search.SearchResults
import ai.senscience.nexus.delta.sdk.model.search.SearchResults.searchResultsJsonLdEncoder
import ai.senscience.nexus.delta.sourcing.ProgressStatistics
import ai.senscience.nexus.delta.sourcing.model.FailedElemLog
import ai.senscience.nexus.delta.sourcing.model.FailedElemLog.FailedElemData
import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder
import io.circe.syntax.EncoderOps
import org.http4s.Uri

package object indexing {

  given Encoder.AsObject[ProgressStatistics] =
    deriveEncoder[ProgressStatistics].mapJsonObject(_.add(keywords.tpe, "ViewStatistics".asJson))

  given JsonLdEncoder[ProgressStatistics] =
    JsonLdEncoder.computeFromCirce(ContextValue(contexts.statistics))

  private val failedElemContext: ContextValue = ContextValue(contexts.error)

  given Encoder.AsObject[FailedElemData] =
    deriveEncoder[FailedElemData].mapJsonObject(_.remove("entityType"))

  given JsonLdEncoder[FailedElemData] =
    JsonLdEncoder.computeFromCirce(failedElemContext)

  given Encoder.AsObject[FailedElemLog] = deriveEncoder[FailedElemLog]

  type FailedElemSearchResults = SearchResults[FailedElemData]

  def failedElemSearchJsonLdEncoder(pagination: Pagination, uri: Uri)(using
      BaseUri
  ): JsonLdEncoder[FailedElemSearchResults] =
    searchResultsJsonLdEncoder[FailedElemData](failedElemContext, pagination, uri)
}
