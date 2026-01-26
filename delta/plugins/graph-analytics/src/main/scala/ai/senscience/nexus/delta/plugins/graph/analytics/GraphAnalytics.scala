package ai.senscience.nexus.delta.plugins.graph.analytics

import ai.senscience.nexus.delta.elasticsearch.client.{ElasticSearchClient, ElasticSearchRequest, IndexLabel}
import ai.senscience.nexus.delta.plugins.graph.analytics.config.GraphAnalyticsConfig.TermAggregationsConfig
import ai.senscience.nexus.delta.plugins.graph.analytics.indexing.{propertiesAggQuery, relationshipsAggQuery}
import ai.senscience.nexus.delta.plugins.graph.analytics.model.GraphAnalyticsRejection.InvalidPropertyType
import ai.senscience.nexus.delta.plugins.graph.analytics.model.PropertiesStatistics.propertiesDecoderFromEsAggregations
import ai.senscience.nexus.delta.plugins.graph.analytics.model.{AnalyticsGraph, PropertiesStatistics}
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sdk.jsonld.ExpandIri
import ai.senscience.nexus.delta.sdk.model.IdSegment
import ai.senscience.nexus.delta.sdk.projects.FetchContext
import ai.senscience.nexus.delta.sdk.syntax.*
import ai.senscience.nexus.delta.sourcing.model.ProjectRef
import cats.data.NonEmptySeq
import cats.effect.IO
import cats.syntax.all.*
import io.circe.Decoder

trait GraphAnalytics {

  /**
    * The relationship statistics between different types for the passed ''projectRef''
    */
  def relationships(projectRef: ProjectRef): IO[AnalyticsGraph]

  /**
    * The properties statistics of the passed ''project'' and type ''tpe''.
    */
  def properties(projectRef: ProjectRef, tpe: IdSegment): IO[PropertiesStatistics]

}

object GraphAnalytics {

  final def apply(
      client: ElasticSearchClient,
      fetchContext: FetchContext,
      prefix: String,
      config: TermAggregationsConfig
  ): GraphAnalytics =
    new GraphAnalytics {

      private val expandIri: ExpandIri[InvalidPropertyType] = new ExpandIri(InvalidPropertyType.apply)

      private def propertiesAggQueryFor(tpe: Iri) =
        propertiesAggQuery(config).map(_.replace("@type" -> "{{type}}", tpe)).map(ElasticSearchRequest(_))

      override def relationships(projectRef: ProjectRef): IO[AnalyticsGraph] =
        for {
          _         <- fetchContext.onRead(projectRef)
          request   <- relationshipsAggQuery(config).map(ElasticSearchRequest(_))
          indexValue = index(prefix, projectRef).value
          stats     <- client.searchAs[AnalyticsGraph](request, Set(indexValue))
        } yield stats

      override def properties(
          projectRef: ProjectRef,
          tpe: IdSegment
      ): IO[PropertiesStatistics] = {

        def search(tpe: Iri, idx: IndexLabel, request: ElasticSearchRequest) = {
          given Decoder[PropertiesStatistics] = propertiesDecoderFromEsAggregations(tpe)
          client.searchAs[PropertiesStatistics](request, Set(idx.value))
        }

        for {
          pc     <- fetchContext.onRead(projectRef)
          tpeIri <- expandIri(tpe, pc)
          query  <- propertiesAggQueryFor(tpeIri)
          stats  <- search(tpeIri, index(prefix, projectRef), query)
        } yield stats

      }
    }

  private[analytics] def toPaths(key: String): Either[String, NonEmptySeq[Iri]] =
    key.split(" / ").toVector.foldM(Vector.empty[Iri])((acc, k) => Iri.reference(k).map(acc :+ _)).flatMap {
      case Seq(first, tail*) => Right(NonEmptySeq(first, tail))
      case _                 => Left("Empty Path")
    }

  private[analytics] def index(prefix: String, ref: ProjectRef): IndexLabel =
    IndexLabel.unsafe(s"${prefix}_ga_${ref.organization}_${ref.project}")

  private[analytics] def projectionName(ref: ProjectRef): String = s"ga-$ref"

  private[analytics] def name(iri: Iri): String =
    (iri.fragment orElse iri.lastSegment) getOrElse iri.toString
}
